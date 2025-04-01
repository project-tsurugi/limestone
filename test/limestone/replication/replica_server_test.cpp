/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include "replication/replica_server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <thread>

#include "replication/channel_handler_base.h"
#include "replication/message_error.h"
#include "replication/message_session_begin.h"
#include "replication/replica_connector.h"
#include "replication/socket_io.h"
#include "replication/handler_resources.h"
namespace limestone::testing {

using namespace limestone::replication;


static uint16_t get_free_port() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    uint16_t port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
 }
 
 static sockaddr_in make_listen_addr(uint16_t port) {
     sockaddr_in addr{};
     addr.sin_family = AF_INET;
     addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
     addr.sin_port = htons(port);
     return addr;
 }

 class replica_server_test : public ::testing::Test {
 public:
     static constexpr const char* base_location = "/tmp/replica_server_test";
     boost::filesystem::path location1 = boost::filesystem::path(base_location) / "replica1";
     boost::filesystem::path location2 = boost::filesystem::path(base_location) / "replica2";

     void SetUp() override {
         if (boost::filesystem::exists(base_location)) {
             boost::filesystem::permissions(base_location, boost::filesystem::owner_all);
         }
         boost::filesystem::remove_all(base_location);
         boost::filesystem::create_directories(location1);
         boost::filesystem::create_directories(location2);
     }

     void TearDown() override {
         if (boost::filesystem::exists(base_location)) {
             boost::filesystem::permissions(base_location, boost::filesystem::owner_all);
         }
         boost::filesystem::remove_all(base_location);
     }
 };

 class test_session_handler : public limestone::replication::channel_handler_base {
 public:
     test_session_handler(limestone::replication::replica_server& server, socket_io& io, std::promise<bool>& invoked) noexcept : channel_handler_base(server, io), invoked_(invoked) {}

 protected:
     limestone::replication::validation_result authorize() override { return limestone::replication::validation_result::success(); }
     limestone::replication::validation_result validate_initial(std::unique_ptr<limestone::replication::replication_message> /*req*/) override {
         invoked_.set_value(true);
         return limestone::replication::validation_result::success();
     }
     void send_initial_ack() const override {}
     void dispatch(limestone::replication::replication_message& /*msg*/, limestone::replication::handler_resources& /*resources*/) override {}

 private:
     std::promise<bool>& invoked_;
 };

 TEST_F(replica_server_test, initialize_does_not_throw) {
     replication::replica_server server;
     EXPECT_NO_THROW(server.initialize(location1));
 }
 
 TEST_F(replica_server_test, start_listener_succeeds) {
     replication::replica_server server;
     server.initialize(location1);
     auto addr = make_listen_addr(get_free_port());
     EXPECT_TRUE(server.start_listener(addr));
     server.shutdown();
 }
 
 TEST_F(replica_server_test, start_listener_fails_if_port_in_use) {
    replication::replica_server s1;
    s1.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(s1.start_listener(addr));

    replication::replica_server s2;
    s2.initialize(location2);
    EXPECT_FALSE(s2.start_listener(addr));  // should fail but not abort

    s1.shutdown();
}

 
 TEST_F(replica_server_test, accept_loop_can_be_shutdown) {
     replication::replica_server server;
     server.initialize(location1);
     auto addr = make_listen_addr(get_free_port());
     ASSERT_TRUE(server.start_listener(addr));
 
     std::thread accept_thread([&server]() {
         server.accept_loop();
     });
 
     std::this_thread::sleep_for(std::chrono::milliseconds(50));
     EXPECT_NO_THROW(server.shutdown());
     accept_thread.join();
 }
 
 TEST_F(replica_server_test, start_listener_and_client_connect_disconnect) {
    replication::replica_server server;
    server.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    // Allow time for the listener to be ready (can be removed if eventfd is used)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    replication::replica_connector client;
    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    server.shutdown();
    accept_thread.join();
}

TEST_F(replica_server_test, no_handler_returns_error) {
    replication::replica_server server;
    server.initialize(location1);
    server.clear_handlers();
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    auto request = message_session_begin::create();
    static_cast<message_session_begin*>(request.get())->set_param("config", 42);
    EXPECT_TRUE(client.send_message(*request));

    auto response = client.receive_message();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_message_type_id(), message_type_id::COMMON_ERROR);

    client.close_session();
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_test, registered_handler_is_called) {
    replication::replica_server server;
    server.initialize(location1);
    server.clear_handlers();
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::promise<bool> invoked;
    
    server.register_handler(replication::message_type_id::SESSION_BEGIN,
        [&server, &invoked](socket_io& io) {
            return std::make_shared<test_session_handler>(server, io, invoked);
        });
    

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));
    auto request = message_session_begin::create();
    static_cast<message_session_begin*>(request.get())->set_param("config", 100);
    EXPECT_TRUE(client.send_message(*request));

    client.close_session();
    EXPECT_TRUE(invoked.get_future().get());

    server.shutdown();
    server_thread.join();
}


TEST_F(replica_server_test, shutdown_before_accept_loop_starts) {
    replication::replica_server server;
    server.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    // Call shutdown before starting the accept loop
    EXPECT_NO_THROW(server.shutdown());

    // Start the accept loop after shutdown
    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    // Try connecting to the server again, should be a failure after shutdown
    replication::replica_connector client;
    EXPECT_FALSE(client.connect_to_server("127.0.0.1", port));

    // Join the accept thread
    accept_thread.join();
}

TEST_F(replica_server_test, listener_restart_multiple_times) {
    replication::replica_server server;
    server.initialize(location1);
    
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);

    // Start listener for the first time
    ASSERT_TRUE(server.start_listener(addr));

    // Try to connect to the server
    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    replication::replica_connector client;
    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    // Shutdown the server
    server.shutdown();
    accept_thread.join();

    // Restart listener for the second time
    ASSERT_TRUE(server.start_listener(addr));

    std::thread accept_thread_2([&server]() {
        server.accept_loop();
    });

    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    // Shutdown the server again
    server.shutdown();
    accept_thread_2.join();
}

TEST_F(replica_server_test, get_datastore_returns_valid_instance) {
    replication::replica_server server;
    server.initialize(location1);
    
    auto& ds = server.get_datastore();
    EXPECT_NE(&ds, nullptr);
}

TEST_F(replica_server_test, get_location_returns_correct_path) {
    replication::replica_server server;
    server.initialize(location1);
    
    auto location = server.get_location();
    EXPECT_EQ(location.string(), location1.string());
}

TEST_F(replica_server_test, mark_control_channel_created_sets_flag) {
    replication::replica_server server;
    server.initialize(location1);

    EXPECT_TRUE(server.mark_control_channel_created());
}

}  // namespace limestone::testing
