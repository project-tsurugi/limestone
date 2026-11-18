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
 * コンパクション境界メッセージが、master からレプリカへ tcp モードのコントロールチャネルで届くことのテスト。
 * レプリカは、同じプロセスのスレッドで動かす。
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

    // master でコンパクションを実行する。ローテーションは境界 epoch までの永続化を待つので、待つ間は epoch を進める
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

    // レプリカが、BLOB の ID の境界が expected である境界を保持するまで待つ
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

// master は、コンパクションの完了のたびに、境界 epoch と BLOB の ID の境界をレプリカへ送る。レプリカは最新の
// 1 組を保持する
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

// 対象がなくてスキップしたコンパクションでは、境界は送られない
TEST_F(compaction_boundary_propagation_test, skipped_compaction_sends_nothing) {
    advance_epoch();
    write_entry("k1", "v1");
    master_->set_next_blob_id(1234);
    run_compaction_on_master();
    ASSERT_TRUE(wait_for_replica_blob_id_boundary(1234, std::chrono::seconds(10)));

    // 新しい WAL がないので、このコンパクションはスキップされる
    master_->set_next_blob_id(5678);
    run_compaction_on_master();

    // 後続の GROUP_COMMIT がレプリカに届いた時点で、境界メッセージが送られていれば届いている
    write_entry("k2", "v2");
    advance_epoch();
    EXPECT_EQ(get_epoch(replica_location), get_epoch(master_location));
    EXPECT_EQ(replica_boundary()->blob_id_boundary, 1234U);
}

// GROUP_COMMIT とコンパクション境界メッセージを並行して送っても、メッセージが壊れない。GROUP_COMMIT の応答の
// 対応もずれない
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
    // master が最後に確定した epoch が、レプリカの epoch ファイルに届いている
    const epoch_id_type master_durable_epoch = get_epoch(master_location);
    EXPECT_GE(master_durable_epoch, count);
    EXPECT_EQ(get_epoch(replica_location), master_durable_epoch);
    EXPECT_EQ(server_.get_datastore().get_impl()->get_received_epoch(), master_durable_epoch);
}

// shutdown() の後は、コンパクション境界メッセージを送らない (閉じたコントロールチャネルに触れない)
TEST_F(compaction_boundary_propagation_test, nothing_is_sent_after_shutdown) {
    advance_epoch();
    master_->get_impl()->send_compaction_boundary(1, 10);
    ASSERT_TRUE(wait_for_replica_blob_id_boundary(10, std::chrono::seconds(10)));

    master_->shutdown();
    master_->get_impl()->send_compaction_boundary(2, 20);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(replica_boundary()->blob_id_boundary, 10U);
}

// shutdown() と並行して送っても、異常が起きない
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
