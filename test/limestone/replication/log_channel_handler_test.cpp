#include "replication/log_channel_handler.h"

#include <pthread.h>

#include "gtest/gtest.h"
#include "replication/channel_handler_base.h"
#include "replication/message_ack.h"
#include "replication/message_error.h"
#include "replication/message_log_channel_create.h"
#include "replication/replica_server.h"
#include "replication/socket_io.h"
#include "replication/validation_result.h"
#include "test_message.h"
namespace limestone::testing {

using namespace limestone::replication;

class dummy_server {};

class log_channel_handler_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/replica_server_test";

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

TEST_F(log_channel_handler_test, validate_initial_and_dispatch_succeeds) {
    replica_server server{};
    server.initialize(base_location);
    socket_io out("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), out);

    auto msg = std::make_unique<message_log_channel_create>();
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_TRUE(result.ok());

    const auto& channel = handler.get_log_channel();
    EXPECT_NE(&channel, nullptr);

    
    test_message test_msg{};
    replication_message::send(out, test_msg);
    socket_io in(out.get_out_string());
    test_message::post_receive_called = false;
    testable_log_handler handler2(reinterpret_cast<replica_server&>(server), in);
    EXPECT_THROW({ handler2.process_loop(); }, limestone_io_exception); // Exception is thrown when the stream becomes unreadable, but it is ignored
    ASSERT_EQ(test_message::post_receive_called, true);
}

TEST_F(log_channel_handler_test, authorize_succeeds_then_fails_at_limit_boundary) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    // Set the counter to one before the maximum limit
    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT - 1);

    // First call: should succeed and assign the final valid ID
    auto result1 = handler.authorize();
    EXPECT_TRUE(result1.ok());

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "logch99999");  // last valid thread name

    // Second call: should fail because it exceeds the maximum allowed count
    auto result2 = handler.authorize();
    EXPECT_FALSE(result2.ok());
    EXPECT_EQ(result2.error_code(), 1);
    EXPECT_EQ(result2.error_message(), "Too many log channels: cannot assign more");
}

TEST_F(log_channel_handler_test, authorize_fails_when_exceeded) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT);
    auto result = handler.authorize();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), 1);
}

TEST_F(log_channel_handler_test, validate_fails_on_wrong_type) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    auto wrong = std::make_unique<message_ack>();
    auto result = handler.validate_initial(std::move(wrong));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), 2);
}

TEST_F(log_channel_handler_test, validate_fails_on_failed_cast) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    class bad_message : public replication_message {
        message_type_id get_message_type_id() const override {
            return message_type_id::LOG_CHANNEL_CREATE;
        }
        void send_body(socket_io&) const override {}
        void receive_body(socket_io&) override {}
        void post_receive(handler_resources&) override {}
    };

    auto msg = std::make_unique<bad_message>();
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), 3);
}

TEST_F(log_channel_handler_test, send_initial_ack_sends_ack_message) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.send_initial_ack();

    socket_io reader(io.get_out_string());
    auto msg = replication_message::receive(reader);
    auto* ack = dynamic_cast<message_ack*>(msg.get());
    ASSERT_NE(ack, nullptr);
}

}  // namespace limestone::testing