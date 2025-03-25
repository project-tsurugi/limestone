#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <arpa/inet.h>
#include <thread>

#include "replication/replica_server.h"
#include "replication/replica_connector.h"
#include "test_message.h"

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

    // Register echo handler for TESTING message
    server.register_handler(replication::message_type_id::TESTING,
        [](int fd, std::unique_ptr<replication::replication_message> msg) {
            replication::socket_io io(fd);
            replication::replication_message::send(io, *msg);
            io.flush();
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

    client.close_session();
    server.shutdown();
    server_thread.join();
}

}  // namespace limestone::testing
