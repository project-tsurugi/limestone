#include "replication/log_channel_handler.h"

#include <pthread.h>

#include "gtest/gtest.h"
#include "replication/channel_handler_base.h"
#include "replication/message_error.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_log_entries.h"
#include "replication/replica_server.h"
#include "replication/replication_message_io.h"
#include "replication/tcp_replication_message_io.h"
#include "replication/validation_result.h"
#include "rdma/rdma_replication_message_io.h"
#include "rdma/rdma_send_stream_base.h"
#include <boost/filesystem.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "test_message.h"
#include "replication_test_helper.h"
#include "test_rdma_frame_buffer.h"
#include "test_root.h"
namespace limestone::testing {

using namespace limestone::replication;

class dummy_server {};

class log_channel_handler_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/log_channel_handler_test";

    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
    }

    void TearDown() override { boost::filesystem::remove_all(base_location); }
};

class testable_log_handler : public log_channel_handler {
public:
    using log_channel_handler::log_channel_handler;
    using log_channel_handler::validate_initial;
    using log_channel_handler::send_initial_ack;
    using log_channel_handler::set_log_channel_id_counter_for_test;
    using log_channel_handler::authorize;
    using log_channel_handler::process_loop;
};

namespace {

class capturing_rdma_send_stream : public rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<rdma_frame_buffer_base> acquire_frame_buffer(
            std::size_t max_payload,
            std::size_t min_capacity) noexcept override {
        auto capacity = granted_frame_capacity(max_payload);
        if (max_frame_capacity_ != 0U) {
            capacity = std::min(capacity, max_frame_capacity_);
        }
        // A real stream retries until it can grant min_capacity, so it never hands back an
        // undersized frame. Honour that contract here too.
        capacity = std::max(capacity, min_capacity);
        return std::make_unique<test_rdma_frame_buffer>(capacity);
    }

    [[nodiscard]] send_result submit_frame_buffer(
            rdma_frame_buffer_base& frame,
            std::size_t             payload_size) override {
        auto& test_frame = dynamic_cast<test_rdma_frame_buffer&>(frame);
        calls_.emplace_back(test_frame.take_written(payload_size));
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        return {true, ""};
    }

    /**
     * @brief Upper bound on the capacity granted by acquire_frame_buffer().
     *
     * A value of 0 grants the full requested payload. A non-zero value caps the grant,
     * forcing callers to split their payload across several frames as a real send ring
     * would.
     */
    std::size_t max_frame_capacity_{};
    std::vector<std::vector<std::uint8_t>> calls_{};
};

}  // namespace

TEST_F(log_channel_handler_test, validate_initial_and_dispatch_succeeds) {
    replica_server server{};
    server.initialize(base_location);
    replication_message_io out("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), out);

    auto msg = std::make_unique<message_log_channel_create>(1234U);
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_TRUE(result.ok());

    const auto& channel = handler.get_log_channel();
    EXPECT_NE(&channel, nullptr);

    
    test_message test_msg{};
    replication_message::send(out, test_msg);
    replication_message_io in(out.get_out_string());
    test_message::post_receive_called = false;
    testable_log_handler handler2(reinterpret_cast<replica_server&>(server), in);
    EXPECT_THROW({ handler2.process_loop(); }, limestone_io_exception); // Exception is thrown when the stream becomes unreadable, but it is ignored
    ASSERT_EQ(test_message::post_receive_called, true);
}

TEST_F(log_channel_handler_test, authorize_succeeds_then_fails_at_limit_boundary) {
    dummy_server server;
    replication_message_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    // Set the counter to one before the maximum limit
    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT - 1);

    // First call: should succeed and assign the final valid ID
    auto result1 = handler.authorize();
    EXPECT_TRUE(result1.ok());

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "logch09999");  // last valid thread name

    // Second call: should fail because it exceeds the maximum allowed count
    auto result2 = handler.authorize();
    EXPECT_FALSE(result2.ok());
    EXPECT_EQ(result2.error_code(), message_error::log_channel_error_too_many_channels);
    EXPECT_EQ(result2.error_message(), "Too many log channels: cannot assign more");
}

TEST_F(log_channel_handler_test, authorize_fails_when_exceeded) {
    dummy_server server;
    replication_message_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT);
    auto result = handler.authorize();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_too_many_channels);
}

TEST_F(log_channel_handler_test, validate_fails_on_wrong_type) {
    dummy_server server;
    replication_message_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    auto wrong = std::make_unique<message_ack>();
    auto result = handler.validate_initial(std::move(wrong));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_invalid_type);
}

TEST_F(log_channel_handler_test, validate_fails_on_failed_cast) {
    dummy_server server;
    replication_message_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    class bad_message : public replication_message {
        message_type_id get_message_type_id() const override {
            return message_type_id::LOG_CHANNEL_CREATE;
        }
        void send_body(replication_message_io&) const override {}
        void receive_body(replication_message_io&) override {}
        void post_receive(handler_resources&) override {}
    };

    auto msg = std::make_unique<bad_message>();
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_bad_cast);
}

TEST_F(log_channel_handler_test, send_initial_ack_sends_ack_message) {
    dummy_server server;
    replication_message_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.send_initial_ack();

    replication_message_io reader(io.get_out_string());
    auto msg = replication_message::receive(reader);
    auto* ack = dynamic_cast<message_ack*>(msg.get());
    ASSERT_NE(ack, nullptr);
}

TEST_F(log_channel_handler_test,
       rdma_replication_message_io_send_blob_with_empty_staged_buffer_sends_blob_only) {
    constexpr const char* sender_dir = "/tmp/log_channel_handler_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    blob_id_type blob_id = 57U;
    std::string const blob_content = "rdma_replication_message_io_empty_staged_buffer";
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    capturing_rdma_send_stream stream{};
    rdma_replication_message_io sender_io(stream, *sender_ds);

    sender_io.send_blob(blob_id);

    ASSERT_EQ(stream.calls_.size(), 1U)
        << "empty staged buffer should not produce an extra RDMA send";
    auto const& payload = stream.calls_[0];
    auto it = std::search(
        payload.begin(), payload.end(),
        blob_content.begin(), blob_content.end());
    EXPECT_NE(it, payload.end());

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);
}

TEST_F(log_channel_handler_test,
       rdma_replication_message_io_send_blob_sends_remaining_data_in_later_writes) {
    constexpr const char* sender_dir = "/tmp/log_channel_handler_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    blob_id_type blob_id = 58U;
    std::string const blob_content = "rdma_replication_message_io_blob_payload_split_after_header";
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    capturing_rdma_send_stream stream{};
    stream.max_frame_capacity_ = sizeof(std::uint64_t) + sizeof(std::uint32_t) + 5U;
    rdma_replication_message_io sender_io(stream, *sender_ds);

    sender_io.send_blob(blob_id);

    ASSERT_GE(stream.calls_.size(), 2U)
        << "remaining BLOB data should be sent after the first header write";

    std::vector<std::uint8_t> all_payload;
    for (auto const& call : stream.calls_) {
        all_payload.insert(all_payload.end(), call.begin(), call.end());
    }
    auto it = std::search(
        all_payload.begin(), all_payload.end(),
        blob_content.begin(), blob_content.end());
    EXPECT_NE(it, all_payload.end());

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);
}

}  // namespace limestone::testing
