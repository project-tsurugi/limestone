#include "replication/replica_connector.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <thread>

#include "replication/socket_io.h"
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

static int start_test_server(uint16_t port, bool echo_message, bool close_immediately = false) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(listen_fd, 1);
    return listen_fd;
}

TEST(replica_connector_test, connect_to_nonexistent_server_returns_false) {
    replication::replica_connector connector;
    uint16_t port = get_free_port();
    EXPECT_FALSE(connector.connect_to_server("127.0.0.1", port));
}

TEST(replica_connector_test, send_and_receive_echo_message) {
    uint16_t port = get_free_port();
    int listen_fd = start_test_server(port, true);

    // Server thread: accept one connection, echo back test_message
    std::thread server_thread([listen_fd]() {
        int client_fd = ::accept(listen_fd, nullptr, nullptr);
        replication::socket_io io(client_fd);

        auto incoming = replication::replication_message::receive(io);
        auto* req = static_cast<replication::test_message*>(incoming.get());
        ASSERT_EQ(req->get_data(), "Test Message Data");

        replication::test_message response;
        replication::replication_message::send(io, response);
        io.flush();

        ::close(client_fd);
        ::close(listen_fd);
    });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    replication::test_message request;
    EXPECT_TRUE(client.send_message(request));

    auto reply = client.receive_message();
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->get_message_type_id(), replication::message_type_id::TESTING);

    auto* resp = static_cast<replication::test_message*>(reply.get());
    EXPECT_EQ(resp->get_data(), "Test Message Data");

    client.close_session();
    server_thread.join();
}

TEST(replica_connector_test, receive_returns_null_when_server_closes_immediately) {
    uint16_t port = get_free_port();
    int listen_fd = start_test_server(port, false);

    std::thread server_thread([listen_fd]() {
        int client_fd = ::accept(listen_fd, nullptr, nullptr);
        ::close(client_fd);
        ::close(listen_fd);
    });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));
    auto result = client.receive_message();
    EXPECT_EQ(result, nullptr);

    client.close_session();
    server_thread.join();
}

TEST(replica_connector_test, connect_fails_on_invalid_hostname) {
    replication::replica_connector connector;
    // Invalid host forces getaddrinfo() to fail
    EXPECT_FALSE(connector.connect_to_server("nonexistent.invalid.host", 12345));
}

TEST(replica_connector_test, connect_fails_when_socket_creation_fails) {
    struct rlimit old_limit;
    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &old_limit), 0);

    // Temporarily set soft limit to 0 so socket() fails
    struct rlimit no_fds{0, old_limit.rlim_max};
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &no_fds), 0);

    replication::replica_connector connector;
    EXPECT_FALSE(connector.connect_to_server("127.0.0.1", 0));

    // Restore original limit
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &old_limit), 0);
}


}  // namespace limestone::testing
