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

#include "replication/replica_connector.h"

namespace limestone::testing {

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

 }  // namespace limestone::testing
 