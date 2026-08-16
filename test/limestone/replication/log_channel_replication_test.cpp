/*
 * Copyright 2022-2025 Project Tsurugi.
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
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <map>

#include "datastore_impl.h"
#include "internal.h"
#include "limestone/api/epoch_id_type.h"
#include "log_entry.h"
#include "replication/log_channel_handler.h"
#include "replication/replica_connector.h"
#include "replication/replica_server.h"
#include "replication/control_channel_handler.h"
#include "replication/message_log_entries.h"
#include "replication_test_helper.h"
#include "test_rdma_frame_buffer.h"
#include "test_root.h"
#include "log_channel_impl.h"
#include "replication/replication_message_io.h"
#include "rdma/rdma_send_stream_base.h"
#include <cstddef>
#include <memory>
#include <vector>

namespace limestone::testing {

using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base = "/tmp/log_channel_replication_test";
constexpr const char* master = "/tmp/log_channel_replication_test/master";
constexpr const char* replica = "/tmp/log_channel_replication_test/replica";

class test_echo_log_channel_handler : public log_channel_handler {
public:
    explicit test_echo_log_channel_handler(replica_server& server, replication_message_io& io) noexcept : log_channel_handler(server, io) {}

protected:
    void dispatch(replication_message& message, handler_resources& resources) override {
        auto& io = resources.get_replication_message_io();
        replication_message::send(io, message);
        io.flush();
    }
};

// This suite presupposes the log channel's per-channel TCP connection, i.e. the
// pure TCP mode. RDMA path coverage lives elsewhere: the send sequence in this
// file's fake-stream injection tests, the receive side in
// rdma_log_channel_receiver_test, and end-to-end in scenario_tcpless_rdma_test
// on RDMA-enabled builds.
class log_channel_replication_test : public ::testing::Test {
protected:
    std::unique_ptr<api::datastore_test> datastore_;

    log_channel* log_channel_{};

    void SetUp() override {
        // Delete and recreate the test directory
        std::string cmd = "rm -rf " + std::string(base);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
        cmd = "mkdir -p " + std::string(master);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot create directory" << std::endl;
        }
        cmd = "mkdir -p " + std::string(replica);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot create directory" << std::endl;
        }

        // If another suite in this binary leaves this env set, the datastore
        // constructor rejects the TCP-mode-with-slots combination at startup,
        // so always unset it as a guard.
        unsetenv("REPLICATION_RDMA_SLOTS");

        uint16_t port = get_free_port();
        start_replica_server(port);
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
    }

    void TearDown() override {
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        unsetenv("REPLICATION_RDMA_SLOTS");
        stop_replica_server();
        std::string cmd = "rm -rf " + std::string(base);
        ;
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
    }

    void gen_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(master);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        log_channel_ = &datastore_->create_channel();
    }

    void start_replica_server(uint16_t port) {
        server_.initialize(boost::filesystem::path(replica));
        server_.clear_handlers();
    
        server_.register_handler(message_type_id::SESSION_BEGIN,
            [this](replication_message_io& io) {
                return std::make_shared<control_channel_handler>(server_, io);
            });
    
        server_.register_handler(message_type_id::LOG_CHANNEL_CREATE,
            [this](replication_message_io& io) {
                return std::make_shared<test_echo_log_channel_handler>(server_, io);
            });
    
        auto addr = make_listen_addr(port);
        ASSERT_TRUE(server_.start_listener(addr));
    
        server_thread_ = std::make_unique<std::thread>([this]() {
            server_.accept_loop();
        });
    }
    

    void stop_replica_server() {
        if (log_channel_ != nullptr) {
            auto connector = log_channel_->get_impl()->get_replica_connector();
            if (connector) {
                connector->close_session();
            }
        }
        if (datastore_ != nullptr) {
            datastore_->shutdown();
            datastore_ = nullptr;
        }
        if (server_thread_ && server_thread_->joinable()) {
            server_.shutdown();
            server_thread_->join();
        }
    }

    replication::replica_connector* begin_session_and_get_connector() {
        gen_datastore();
        EXPECT_EQ(datastore_->get_impl()->get_control_channel(), nullptr);
        datastore_->ready();
        datastore_->switch_epoch(111);

        EXPECT_NE(log_channel_->get_impl()->get_replica_connector(), nullptr);
        log_channel_->begin_session();
        EXPECT_NE(log_channel_->get_impl()->get_replica_connector(), nullptr);
        auto connector = log_channel_->get_impl()->get_replica_connector();
        auto msg = connector->receive_message();
        auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
        EXPECT_NE(log_entry, nullptr);
        EXPECT_EQ(log_entry->get_epoch_id(), 111);
        EXPECT_EQ(log_entry->get_entries().size(), 0);
        EXPECT_EQ(log_entry->has_session_begin_flag(), true);
        EXPECT_EQ(log_entry->has_session_end_flag(), false);
        EXPECT_EQ(log_entry->has_flush_flag(), false);
        return connector;
    };
    
private:
    replication::replica_server server_;
    std::unique_ptr<std::thread> server_thread_;
};

class fake_rdma_send_stream : public limestone::replication::rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<limestone::replication::rdma_frame_buffer_base> acquire_frame_buffer(
            std::size_t max_payload,
            std::size_t min_capacity) noexcept override {
        return std::make_unique<test_rdma_frame_buffer>(
            std::max(granted_frame_capacity(max_payload), min_capacity));
    }

    [[nodiscard]] send_result submit_frame_buffer(
            limestone::replication::rdma_frame_buffer_base& frame,
            std::size_t payload_size) override {
        send_count_++;
        if (payload_size > frame.capacity()) {
            ADD_FAILURE() << "payload_size exceeds frame capacity: payload_size=" << payload_size
                          << " capacity=" << frame.capacity();
            return { false, "payload_size exceeds frame capacity", 0U };
        }
        last_payload_size_ = payload_size;
        return { true, "", payload_size };
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        flush_count_++;
        return { true, "" };
    }

    std::size_t send_count_{};
    std::size_t flush_count_{};
    std::size_t last_payload_size_{};
};

/**
 * @brief An rdma_send_stream that records the payload of every submitted frame,
 *        enabling order and content verification in tests.
 */
class capturing_rdma_send_stream : public limestone::replication::rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<limestone::replication::rdma_frame_buffer_base> acquire_frame_buffer(
            std::size_t max_payload,
            std::size_t min_capacity) noexcept override {
        return std::make_unique<test_rdma_frame_buffer>(
            std::max(granted_frame_capacity(max_payload), min_capacity));
    }

    [[nodiscard]] send_result submit_frame_buffer(
            limestone::replication::rdma_frame_buffer_base& frame,
            std::size_t payload_size) override {
        auto& test_frame = dynamic_cast<test_rdma_frame_buffer&>(frame);
        calls_.emplace_back(test_frame.take_written(payload_size));
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        return {true, ""};
    }

    /// @brief Payload of each submitted frame, in submission order.
    std::vector<std::vector<std::uint8_t>> calls_;
};

TEST_F(log_channel_replication_test, replica_connector_setter_getter) {
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    gen_datastore();
    limestone::api::log_channel& channel = datastore_->create_channel();

    EXPECT_EQ(channel.get_impl()->get_replica_connector(), nullptr);

    auto connector = std::make_unique<limestone::replication::replica_connector>();
    channel.get_impl()->set_replica_connector(std::move(connector));

    EXPECT_NE(channel.get_impl()->get_replica_connector(), nullptr);
}

TEST_F(log_channel_replication_test, replica_connector_disable) {
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    gen_datastore();
    limestone::api::log_channel& channel = datastore_->create_channel();

    auto connector = std::make_unique<limestone::replication::replica_connector>();
    channel.get_impl()->set_replica_connector(std::move(connector));
    EXPECT_NE(channel.get_impl()->get_replica_connector(), nullptr);

    channel.get_impl()->disable_replica_connector();
    EXPECT_EQ(channel.get_impl()->get_replica_connector(), nullptr);
}


TEST_F(log_channel_replication_test, log_channel_begin_session)
{
    auto connector = begin_session_and_get_connector();
}

// TODO: As a result of waiting for ACK at end_session, the test does not pass.
// Since it cannot be easily fixed, it is marked as DISABLED.
TEST_F(log_channel_replication_test, DISABLED_log_channel_end_session) {
    auto connector = begin_session_and_get_connector();
    log_channel_->end_session();
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    EXPECT_EQ(log_entry->get_entries().size(), 0);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), true);
    EXPECT_EQ(log_entry->has_flush_flag(), true);
}

TEST_F(log_channel_replication_test, log_channel_add_entry) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    std::string_view key = "test_key";
    std::string_view value = "test_value";
    write_version_type write_version(111, 1);  // Example version

    log_channel_->add_entry(storage_id, key, value, write_version);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].key, key);
    EXPECT_EQ(log_entry->get_entries()[0].value, value);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), false);
    EXPECT_EQ(log_entry->has_flush_flag(), false);
}

TEST_F(log_channel_replication_test, log_channel_add_entry_with_large_objects) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    std::string_view key = "test_key";
    std::string_view value = "test_value";
    write_version_type write_version(111, 1);  // Example version
    std::vector<blob_id_type> large_objects = { 456, 789 };
    
    auto blob_file456 = datastore_->get_blob_file(456).path();
    boost::filesystem::create_directories(blob_file456.parent_path());
    std::ofstream ofs(blob_file456.string());
    ofs << "Dummy data for blob 456";
    ofs.close();

    auto blob_file789 = datastore_->get_blob_file(789).path();
    boost::filesystem::create_directories(blob_file789.parent_path());
    std::ofstream ofs2(blob_file789.string());
    ofs2 << "Dummy data for blob 789";
    ofs2.close();


    log_channel_->add_entry(storage_id, key, value, write_version, large_objects);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].key, key);
    EXPECT_EQ(log_entry->get_entries()[0].value, value);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->get_entries()[0].blob_ids.size(), large_objects.size());
    EXPECT_EQ(log_entry->get_entries()[0].blob_ids[0], 456);
    EXPECT_EQ(log_entry->get_entries()[0].blob_ids[1], 789);
}

TEST_F(log_channel_replication_test, log_channel_remove_entry) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    std::string_view key = "test_key";
    write_version_type write_version(111, 1);  // Example version

    log_channel_->remove_entry(storage_id, key, write_version);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].key, key);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), false);
    EXPECT_EQ(log_entry->has_flush_flag(), false);
}

TEST_F(log_channel_replication_test, rdma_send_reuses_serializer_and_flushes) {
    auto connector = begin_session_and_get_connector();
    auto* impl = log_channel_->get_impl();

    auto rdma_stream = std::make_unique<fake_rdma_send_stream>();
    auto* rdma_stream_ptr = rdma_stream.get();
    impl->set_rdma_send_stream(std::move(rdma_stream));
    EXPECT_TRUE(impl->has_rdma_send_stream());

    impl->send_replica_message(111, [&](replication::message_log_entries& msg) {
        msg.set_session_begin_flag(true);
    });
    impl->send_replica_message(111, [&](replication::message_log_entries& msg) {
        msg.set_flush_flag(true);
    });

    // Small messages are accumulated in the buffer and not yet sent via send_bytes.
    EXPECT_EQ(rdma_stream_ptr->send_count_, 0U);

    impl->flush_rdma_stream();

    // After flush, accumulated messages are sent in a single send_bytes call, then rdma flush.
    EXPECT_EQ(rdma_stream_ptr->send_count_, 1U);
    EXPECT_GT(rdma_stream_ptr->last_payload_size_, 0U);
    EXPECT_EQ(rdma_stream_ptr->flush_count_, 1U);
}

TEST_F(log_channel_replication_test, rdma_flush_async_executes) {
    api::log_channel_impl impl;

    auto rdma_stream = std::make_unique<fake_rdma_send_stream>();
    auto* rdma_stream_ptr = rdma_stream.get();
    impl.set_rdma_send_stream(std::move(rdma_stream));
    EXPECT_TRUE(impl.has_rdma_send_stream());

    auto fut = impl.flush_rdma_stream_async();
    fut.get();

    EXPECT_EQ(rdma_stream_ptr->flush_count_, 1U);
}

TEST_F(log_channel_replication_test, rdma_send_triggers_flush_when_threshold_exceeded) {
    auto connector = begin_session_and_get_connector();
    auto* impl = log_channel_->get_impl();

    auto rdma_stream = std::make_unique<fake_rdma_send_stream>();
    auto* ptr = rdma_stream.get();
    impl->set_rdma_send_stream(std::move(rdma_stream));
    EXPECT_TRUE(impl->has_rdma_send_stream());

    // Accumulate messages until the threshold is exceeded and send_bytes is triggered.
    // Each message carries ~4KB value to reach the 56KB threshold within a reasonable loop count.
    const std::string large_value(4000, 'x');
    int iterations = 0;
    while (ptr->send_count_ == 0) {
        impl->send_replica_message(111, [&](replication::message_log_entries& msg) {
            msg.add_normal_entry(1U, "k", large_value,
                                 write_version_type{api::epoch_id_type{111}, 0U});
        });
        ++iterations;
        ASSERT_LT(iterations, 100) << "threshold was never reached";
    }
    EXPECT_EQ(ptr->send_count_, 1U);
    EXPECT_GE(ptr->last_payload_size_, api::log_channel_impl::rdma_send_buffer_threshold);
}

TEST_F(log_channel_replication_test, rdma_send_buffer_resets_after_threshold_flush) {
    auto connector = begin_session_and_get_connector();
    auto* impl = log_channel_->get_impl();

    auto rdma_stream = std::make_unique<fake_rdma_send_stream>();
    auto* ptr = rdma_stream.get();
    impl->set_rdma_send_stream(std::move(rdma_stream));

    // Reach the threshold to trigger a flush.
    const std::string large_value(4000, 'x');
    while (ptr->send_count_ == 0) {
        impl->send_replica_message(111, [&](replication::message_log_entries& msg) {
            msg.add_normal_entry(1U, "k", large_value,
                                 write_version_type{api::epoch_id_type{111}, 0U});
        });
    }
    std::size_t count_after_first_flush = ptr->send_count_;

    // A small message after flush must NOT trigger another send_bytes call.
    impl->send_replica_message(111, [&](replication::message_log_entries& msg) {
        msg.add_normal_entry(1U, "k2", "v2",
                             write_version_type{api::epoch_id_type{111}, 0U});
    });
    EXPECT_EQ(ptr->send_count_, count_after_first_flush);
}

TEST_F(log_channel_replication_test, log_channel_add_storage) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    write_version_type write_version(111, 1);  // Example version

    log_channel_->add_storage(storage_id, write_version);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), false);
    EXPECT_EQ(log_entry->has_flush_flag(), false);
}

TEST_F(log_channel_replication_test, log_channel_remove_storage) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    write_version_type write_version(111, 1);  // Example version

    log_channel_->remove_storage(storage_id, write_version);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), false);
    EXPECT_EQ(log_entry->has_flush_flag(), false);
}

TEST_F(log_channel_replication_test, log_channel_truncate_storage) {
    auto connector = begin_session_and_get_connector();
    storage_id_type storage_id = 123;
    write_version_type write_version(111, 1);  // Example version

    log_channel_->truncate_storage(storage_id, write_version);
    auto msg = connector->receive_message();
    auto log_entry = dynamic_cast<message_log_entries*>(msg.get());
    ASSERT_NE(log_entry, nullptr);
    ASSERT_EQ(log_entry->get_epoch_id(), 111);
    ASSERT_EQ(log_entry->get_entries().size(), 1);
    EXPECT_EQ(log_entry->get_entries()[0].storage_id, storage_id);
    EXPECT_EQ(log_entry->get_entries()[0].write_version, write_version);
    EXPECT_EQ(log_entry->has_session_begin_flag(), false);
    EXPECT_EQ(log_entry->has_session_end_flag(), false);
    EXPECT_EQ(log_entry->has_flush_flag(), false);
}


// Verify that when a BLOB message is sent via RDMA, any non-BLOB data accumulated
// in rdma_serializer_io_ is flushed before the BLOB payload is sent, preserving
// message ordering.
TEST_F(log_channel_replication_test, rdma_send_blob_flushes_pending_buffer_first) {
    auto connector = begin_session_and_get_connector();
    auto* impl = log_channel_->get_impl();

    auto rdma_stream = std::make_unique<capturing_rdma_send_stream>();
    auto* ptr = rdma_stream.get();
    impl->set_rdma_send_stream(std::move(rdma_stream));

    // Accumulate a small non-blob message in rdma_serializer_io_ (below threshold).
    impl->send_replica_message(111, [](replication::message_log_entries& msg) {
        msg.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{111}, 0U});
    });
    EXPECT_TRUE(ptr->calls_.empty()) << "No RDMA call expected before threshold is reached";

    // Create BLOB file.
    blob_id_type blob_id = 42U;
    auto blob_path = datastore_->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << "blob_ordering_test";
    }

    // Sending a BLOB message must first flush the buffered non-blob data.
    impl->send_replica_message(111, [blob_id](replication::message_log_entries& msg) {
        msg.add_normal_with_blob(1U, "bk", "bv",
            write_version_type{epoch_id_type{111}, 0U}, {blob_id});
    });

    std::string const expected_ordering = "blob_ordering_test";

    // The staged non-blob buffer must go out first, ahead of the BLOB payload, so the
    // first frame cannot carry any of the BLOB content.
    ASSERT_GE(ptr->calls_.size(), 2U) << "No RDMA send found after the non-blob flush";
    auto const& first_frame = ptr->calls_.front();
    EXPECT_EQ(
        std::search(first_frame.begin(), first_frame.end(),
            expected_ordering.begin(), expected_ordering.end()),
        first_frame.end())
        << "The first frame must flush the pending non-blob buffer, not the BLOB payload";

    // Verify that the RDMA stream actually contains the BLOB file content.
    std::vector<std::uint8_t> all_rdma_blob_bytes;
    for (auto const& call : ptr->calls_) {
        all_rdma_blob_bytes.insert(all_rdma_blob_bytes.end(), call.begin(), call.end());
    }
    auto it = std::search(
        all_rdma_blob_bytes.begin(), all_rdma_blob_bytes.end(),
        expected_ordering.begin(), expected_ordering.end());
    EXPECT_NE(it, all_rdma_blob_bytes.end())
        << "BLOB content not found in RDMA-transmitted bytes for ordering test";
}

// Verify that the blob file content is correctly transmitted via RDMA. The raw
// bytes sent through the RDMA stream must contain the verbatim blob data.
TEST_F(log_channel_replication_test, rdma_send_blob_data_content_is_transmitted) {
    auto connector = begin_session_and_get_connector();
    auto* impl = log_channel_->get_impl();

    auto rdma_stream = std::make_unique<capturing_rdma_send_stream>();
    auto* ptr = rdma_stream.get();
    impl->set_rdma_send_stream(std::move(rdma_stream));

    // Create BLOB file with known content.
    blob_id_type blob_id = 77U;
    std::string const blob_content = "rdma_blob_test_content_12345";
    auto blob_path = datastore_->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    impl->send_replica_message(111, [blob_id](replication::message_log_entries& msg) {
        msg.add_normal_with_blob(1U, "k", "v",
            write_version_type{epoch_id_type{111}, 0U}, {blob_id});
    });

    // The blob data is transmitted across RDMA frames. Collect all bytes from those
    // frames and use std::search for a binary-safe content check.
    std::vector<std::uint8_t> all_rdma_bytes;
    for (auto const& call : ptr->calls_) {
        all_rdma_bytes.insert(all_rdma_bytes.end(), call.begin(), call.end());
    }
    auto it = std::search(
        all_rdma_bytes.begin(), all_rdma_bytes.end(),
        blob_content.begin(), blob_content.end());
    EXPECT_NE(it, all_rdma_bytes.end())
        << "BLOB content not found in RDMA-transmitted bytes";
}

} // namespace limestone::testing
