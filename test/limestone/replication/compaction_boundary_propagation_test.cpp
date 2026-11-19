/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <boost/filesystem.hpp>

#include "replication/replica_server.h"
#include "replication/replication_endpoint.h"

#include "test_root.h"

#include "datastore_impl.h"
#include "replication_test_helper.h"

namespace limestone::testing {

using limestone::api::datastore_impl;
using limestone::api::epoch_id_type;

/**
 * Tests that a compaction boundary message reaches the replica from the master over the control channel of the
 * TCP mode. The replica runs on a thread of the same process.
 */
class compaction_boundary_propagation_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/compaction_boundary_propagation_test";
    static constexpr const char* master_location = "/tmp/compaction_boundary_propagation_test/master";
    static constexpr const char* replica_location = "/tmp/compaction_boundary_propagation_test/replica";

    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(master_location);
        boost::filesystem::create_directories(replica_location);
        unsetenv("REPLICATION_RDMA_SLOTS");

        const std::uint16_t port = get_free_port();
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
        limestone::replication::replication_endpoint endpoint{};
        server_.initialize(boost::filesystem::path(replica_location));
        ASSERT_TRUE(server_.start_listener(endpoint.get_sockaddr()));
        replica_thread_ = std::thread([this]() { server_.accept_loop(); });

        limestone::api::configuration conf{};
        conf.set_data_location(master_location);
        master_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &master_->create_channel();
        master_->ready();
    }

    void TearDown() override {
        master_ = nullptr;
        if (replica_thread_.joinable()) {
            server_.shutdown();
            replica_thread_.join();
        }
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        boost::filesystem::remove_all(base_location);
    }

    void advance_epoch() {
        ++epoch_;
        master_->switch_epoch(epoch_);
    }

    void write_entry(const std::string& key, const std::string& value) {
        lc0_->begin_session();
        lc0_->add_entry(1, key, value, {epoch_, 0});
        lc0_->end_session();
        advance_epoch();
    }

    // Runs a compaction on the master. The rotation waits for the persistence up to the boundary epoch, so the
    // epoch is advanced while waiting
    void run_compaction_on_master() {
        std::future<void> future = std::async(std::launch::async, [this]() { master_->compact_with_online(); });
        while (future.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
            advance_epoch();
        }
        future.get();
    }

    [[nodiscard]] std::optional<datastore_impl::compaction_boundary> replica_boundary() {
        return server_.get_datastore().get_impl()->get_received_compaction_boundary();
    }

    // Waits until the replica keeps a boundary whose BLOB ID boundary is expected
    [[nodiscard]] bool wait_for_replica_blob_id_boundary(std::uint64_t expected, std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            const std::optional<datastore_impl::compaction_boundary> boundary = replica_boundary();
            if (boundary.has_value() && boundary->blob_id_boundary == expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    limestone::replication::replica_server server_{};
    std::thread replica_thread_{};
    std::unique_ptr<limestone::api::datastore_test> master_{};
    limestone::api::log_channel* lc0_{};
    epoch_id_type epoch_{0};
};

// Each time a compaction completes, the master sends the boundary epoch and the BLOB ID boundary to the replica.
// The replica keeps the latest pair
TEST_F(compaction_boundary_propagation_test, master_sends_the_boundary_after_each_compaction) {
    advance_epoch();
    write_entry("k1", "v1");
    master_->set_next_blob_id(1234);
    ASSERT_FALSE(replica_boundary().has_value());

    run_compaction_on_master();

    ASSERT_TRUE(wait_for_replica_blob_id_boundary(1234, std::chrono::seconds(10)));
    const epoch_id_type first_boundary_epoch = replica_boundary()->boundary_epoch;
    EXPECT_GT(first_boundary_epoch, 0U);
    EXPECT_LE(first_boundary_epoch, epoch_);

    write_entry("k2", "v2");
    master_->set_next_blob_id(5678);
    run_compaction_on_master();

    ASSERT_TRUE(wait_for_replica_blob_id_boundary(5678, std::chrono::seconds(10)));
    EXPECT_GT(replica_boundary()->boundary_epoch, first_boundary_epoch);
}

// No boundary is sent by a compaction skipped for having no target
TEST_F(compaction_boundary_propagation_test, skipped_compaction_sends_nothing) {
    advance_epoch();
    write_entry("k1", "v1");
    master_->set_next_blob_id(1234);
    run_compaction_on_master();
    ASSERT_TRUE(wait_for_replica_blob_id_boundary(1234, std::chrono::seconds(10)));

    // There is no new WAL, so this compaction is skipped
    master_->set_next_blob_id(5678);
    run_compaction_on_master();

    // When the following GROUP_COMMIT has reached the replica, a boundary message would have reached it too if
    // one had been sent
    write_entry("k2", "v2");
    advance_epoch();
    EXPECT_EQ(get_epoch(replica_location), get_epoch(master_location));
    EXPECT_EQ(replica_boundary()->blob_id_boundary, 1234U);
}

// Sending GROUP_COMMIT and compaction boundary messages concurrently does not corrupt the messages, and the
// responses to GROUP_COMMIT stay paired
TEST_F(compaction_boundary_propagation_test, concurrent_with_group_commit) {
    constexpr std::uint64_t count = 300;
    advance_epoch();

    std::thread boundary_sender([this]() {
        for (std::uint64_t i = 1; i <= count; ++i) {
            master_->get_impl()->send_compaction_boundary(i, i * 10);
        }
    });
    for (std::uint64_t i = 0; i < count; ++i) {
        write_entry("k" + std::to_string(i), "v");
    }
    boundary_sender.join();
    advance_epoch();

    ASSERT_TRUE(wait_for_replica_blob_id_boundary(count * 10, std::chrono::seconds(10)));
    EXPECT_EQ(replica_boundary()->boundary_epoch, count);
    // The last epoch the master fixed has reached the epoch file of the replica
    const epoch_id_type master_durable_epoch = get_epoch(master_location);
    EXPECT_GE(master_durable_epoch, count);
    EXPECT_EQ(get_epoch(replica_location), master_durable_epoch);
    EXPECT_EQ(server_.get_datastore().get_impl()->get_received_epoch(), master_durable_epoch);
}

// No compaction boundary message reaches the replica after shutdown()
TEST_F(compaction_boundary_propagation_test, nothing_is_sent_after_shutdown) {
    advance_epoch();
    master_->get_impl()->send_compaction_boundary(1, 10);
    ASSERT_TRUE(wait_for_replica_blob_id_boundary(10, std::chrono::seconds(10)));

    master_->shutdown();
    master_->get_impl()->send_compaction_boundary(2, 20);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(replica_boundary()->blob_id_boundary, 10U);
}

// A send before the control channel is opened (before ready()) causes no anomaly. It does not reach the
// replica
TEST_F(compaction_boundary_propagation_test, send_before_control_channel_is_opened) {
    master_ = nullptr;
    limestone::api::configuration conf{};
    conf.set_data_location(master_location);
    master_ = std::make_unique<limestone::api::datastore_test>(conf);

    master_->get_impl()->send_compaction_boundary(1, 10);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(replica_boundary().has_value());
}

// A send after the replica stopped causes no anomaly. A failed send does not stop the master
TEST_F(compaction_boundary_propagation_test, send_after_replica_stopped) {
    advance_epoch();
    master_->get_impl()->send_compaction_boundary(1, 10);
    ASSERT_TRUE(wait_for_replica_blob_id_boundary(10, std::chrono::seconds(10)));
    server_.shutdown();
    replica_thread_.join();

    // The first few writes to a socket closed by the peer may succeed, so send repeatedly
    for (std::uint64_t i = 2; i < 50; ++i) {
        master_->get_impl()->send_compaction_boundary(i, i * 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// A send concurrent with shutdown() causes no anomaly
TEST_F(compaction_boundary_propagation_test, send_concurrent_with_shutdown) {
    advance_epoch();
    std::atomic<bool> stop_requested{false};
    std::thread boundary_sender([this, &stop_requested]() {
        std::uint64_t i = 0;
        while (! stop_requested.load()) {
            ++i;
            master_->get_impl()->send_compaction_boundary(i, i);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    master_->shutdown();

    stop_requested.store(true);
    boundary_sender.join();
}

}  // namespace limestone::testing
