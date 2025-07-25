#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <arpa/inet.h>
#include <thread>

#include "replication/replica_server.h"
#include "replication/replica_connector.h"
#include "replication/channel_handler_base.h"
#include "replication/message_session_begin.h"
#include "replication/message_session_begin_ack.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_error.h"
#include "test_message.h"

#include "replication_test_helper.h"

namespace limestone::testing {

using namespace limestone::replication;    



class testing_echo_handler : public limestone::replication::channel_handler_base {
public:
    explicit testing_echo_handler(limestone::replication::replica_server& server, socket_io& io) noexcept : channel_handler_base(server, io) {}

protected:
    validation_result authorize() override { return validation_result::success(); }

    validation_result validate_initial(std::unique_ptr<replication_message> request) override {
        // Store the incoming message so send_initial_ack can echo it
        first_msg_ = std::move(request);
        return validation_result::success();
    }

    void send_initial_ack() const override {
        // Echo the very first request
        replication_message::send(get_socket_io(), *first_msg_);
        get_socket_io() .flush();
    }

    void dispatch(replication_message& /*message*/, handler_resources& /*resources*/) override {
        // No further dispatch
    }

private:
    std::unique_ptr<replication_message> first_msg_;
};

class replica_server_connector_test : public ::testing::Test {
protected:
    boost::filesystem::path location_{"/tmp/replica_server_connector_test"};

    void SetUp() override {
        boost::filesystem::remove_all(location_);
        boost::filesystem::create_directories(location_);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location_);
    }
};

TEST_F(replica_server_connector_test, echo_test_message_between_server_and_connector) {
    replication::replica_server server;
    server.initialize(location_);
    server.clear_handlers();

    // Register shared handler instance instead of lambda
    server.register_handler(replication::message_type_id::TESTING,
        [&server](socket_io& io) {
            return std::make_shared<testing_echo_handler>(server, io);
        });
    

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    limestone::replication::test_message request;
    EXPECT_TRUE(client.send_message(request));

    auto response = client.receive_message();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_message_type_id(), replication::message_type_id::TESTING);

    auto* resp = static_cast<replication::test_message*>(response.get());
    // echo handler returns raw body (no post_receive processing)
    EXPECT_EQ(resp->get_data(), "Test Message Data");

    try { client.close_session(); } catch(...) {}
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_connector_test, control_handler_session_begin_ack) {
    replication::replica_server server;
    server.initialize(location_);

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    auto request = replication::message_session_begin::create();
    static_cast<replication::message_session_begin*>(request.get())->set_param("config", 42);
    EXPECT_TRUE(client.send_message(*request));

    auto response = client.receive_message();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_message_type_id(), replication::message_type_id::SESSION_BEGIN_ACK);

    auto* ack = static_cast<replication::message_session_begin_ack*>(response.get());
    EXPECT_EQ(ack->get_session_secret(), "server_.get_session_secret()");

    try { client.close_session(); } catch(...) {}
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_connector_test, log_handler_initial_ack) {
    replication::replica_server server;
    server.initialize(location_);

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    auto request = replication::message_log_channel_create::create();
    EXPECT_TRUE(client.send_message(*request));

    auto response = client.receive_message();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_message_type_id(), replication::message_type_id::COMMON_ACK);

    try { client.close_session(); } catch(...) {}
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_connector_test, control_handler_rejects_second_session_begin) {
    replication::replica_server server;
    server.initialize(location_);

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    // First session — should get ACK
    replication::replica_connector client1;
    ASSERT_TRUE(client1.connect_to_server("127.0.0.1", port));
    auto first_request = replication::message_session_begin::create();
    EXPECT_TRUE(client1.send_message(*first_request));
    auto first_response = client1.receive_message();
    ASSERT_NE(first_response, nullptr);
    EXPECT_EQ(first_response->get_message_type_id(), replication::message_type_id::SESSION_BEGIN_ACK);
    

    // Second session — should get ERROR
    replication::replica_connector client2;
    ASSERT_TRUE(client2.connect_to_server("127.0.0.1", port));
    auto second_request = replication::message_session_begin::create();
    EXPECT_TRUE(client2.send_message(*second_request));
    auto second_response = client2.receive_message();
    ASSERT_NE(second_response, nullptr);
    EXPECT_EQ(second_response->get_message_type_id(), replication::message_type_id::COMMON_ERROR);

    auto* err = static_cast<limestone::replication::message_error*>(second_response.get());
    EXPECT_EQ(err->get_error_code(), 1);
    // error_message contains “already received”
    EXPECT_NE(err->get_error_message().find("Control channel already created"), std::string::npos);
    
    try { client1.close_session(); } catch(...) {}
    try { client2.close_session(); } catch(...) {}
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_connector_test, control_and_multiple_log_channels_simultaneous) {
    replica_server server;
    server.initialize(location_);

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    // Hold all clients open until the end
    std::vector<replica_connector> clients;

    // 1) Open control session
    clients.emplace_back();
    ASSERT_TRUE(clients.back().connect_to_server("127.0.0.1", port));
    {
        auto request = message_session_begin::create();
        EXPECT_TRUE(clients.back().send_message(*request));
        auto response = clients.back().receive_message();
        ASSERT_NE(response, nullptr);
        EXPECT_EQ(response->get_message_type_id(), message_type_id::SESSION_BEGIN_ACK);
    }

    // 2) Open five log channel sessions
    for (int i = 0; i < 5; ++i) {
        clients.emplace_back();
        ASSERT_TRUE(clients.back().connect_to_server("127.0.0.1", port));
        auto request = message_log_channel_create::create();
        EXPECT_TRUE(clients.back().send_message(*request));
        auto response = clients.back().receive_message();
        ASSERT_NE(response, nullptr);
        EXPECT_EQ(response->get_message_type_id(), message_type_id::COMMON_ACK);
    }

    // Close all sessions
    for (auto& client : clients) {
        try { client.close_session(); } catch(...) {}
    }

    server.shutdown();
    server_thread.join();
}


}  // namespace limestone::testing
