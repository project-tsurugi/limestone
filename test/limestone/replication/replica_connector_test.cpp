#include "replication/replica_connector.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <thread>

#include "replication/socket_io.h"
#include "test_message.h"
#include "replication_test_helper.h"
#include "blob_file_resolver.h"
#include "test_root.h"
namespace limestone::testing {

using limestone::internal::blob_file_resolver;    
constexpr const char* base_directory = "/tmp/test_blob_resolver";

class replica_connector_test : public ::testing::Test {
protected:
    void SetUp() override {
        system(("rm -rf " + std::string(base_directory)).c_str());
        system(("mkdir -p " + std::string(base_directory)).c_str());
        resolver_ = std::make_unique<blob_file_resolver>(boost::filesystem::path(base_directory));
    }

    void TearDown() override {
        resolver_.reset();
        system(("rm -rf " + std::string(base_directory)).c_str());
    }

    std::unique_ptr<blob_file_resolver> resolver_;
};

TEST_F(replica_connector_test, connect_to_nonexistent_server_returns_false) {
    replication::replica_connector connector;
    uint16_t port = get_free_port();
    EXPECT_FALSE(connector.connect_to_server("127.0.0.1", port));
}

TEST_F(replica_connector_test, send_and_receive_echo_message) {
    uint16_t port = get_free_port();
    
    // Mutex to protect listen_fd access
    std::mutex listen_fd_mutex;
    int listen_fd{};
    {
        std::lock_guard<std::mutex> lock(listen_fd_mutex);
        listen_fd = start_test_server(port, true);
    }
    // Server thread: accept one connection, echo back test_message
    std::thread server_thread([&listen_fd, &listen_fd_mutex]() {
        int client_fd;
        {
            std::lock_guard<std::mutex> lock(listen_fd_mutex);  // Lock the mutex for safe access
            client_fd = ::accept(listen_fd, nullptr, nullptr);
        }
        // Server-side logic to handle connection
        replication::socket_io io(client_fd);

        auto incoming = replication::replication_message::receive(io);
        auto* req = static_cast<replication::test_message*>(incoming.get());
        ASSERT_EQ(req->get_data(), "Test Message Data");

        replication::test_message response;
        replication::replication_message::send(io, response);
        io.flush();

        ::close(client_fd);
        // No need to close listen_fd here, as it's shared and managed by the server thread
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

TEST_F(replica_connector_test, receive_returns_null_when_server_closes_immediately) {
    uint16_t port = get_free_port();
    
    // Mutex to protect listen_fd access
    std::mutex listen_fd_mutex;
    int listen_fd{};
    {
        std::lock_guard<std::mutex> lock(listen_fd_mutex);
        listen_fd = start_test_server(port, true);
    }
    // Server thread: accept one connection, echo back test_message
    std::thread server_thread([&listen_fd, &listen_fd_mutex]() {
        int client_fd;
        {
            std::lock_guard<std::mutex> lock(listen_fd_mutex);  // Lock the mutex for safe access
            client_fd = ::accept(listen_fd, nullptr, nullptr);
        }
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

TEST_F(replica_connector_test, connect_fails_on_invalid_hostname) {
    replication::replica_connector connector;
    // Invalid host forces getaddrinfo() to fail
    EXPECT_FALSE(connector.connect_to_server("nonexistent.invalid.host", 12345));
}

TEST_F(replica_connector_test, connect_fails_when_socket_creation_fails) {
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


TEST_F(replica_connector_test, connect_to_server_with_blob_support) {
    uint16_t port = get_free_port();
    
    // Mutex to protect listen_fd access
    std::mutex listen_fd_mutex;
    int listen_fd{};
    {
        std::lock_guard<std::mutex> lock(listen_fd_mutex);
        listen_fd = start_test_server(port, true);
    }
    // Server thread: accept one connection, echo back test_message
    std::thread server_thread([&listen_fd, &listen_fd_mutex]() {
        int client_fd;
        {
            std::lock_guard<std::mutex> lock(listen_fd_mutex);  // Lock the mutex for safe access
            client_fd = ::accept(listen_fd, nullptr, nullptr);
        }
        // Server-side logic to handle connection
        replication::socket_io io(client_fd);

        auto incoming = replication::replication_message::receive(io);
        auto* req = static_cast<replication::test_message*>(incoming.get());
        ASSERT_EQ(req->get_data(), "Test Message Data");

        replication::test_message response;
        replication::replication_message::send(io, response);
        io.flush();

        ::close(client_fd);
        // No need to close listen_fd here, as it's shared and managed by the server thread
    });

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(base_directory);
    boost::filesystem::path metadata_location_path{base_directory};
    limestone::api::configuration conf(data_locations, metadata_location_path);
    auto ds = std::make_unique<limestone::api::datastore_test>(conf);



    replication::replica_connector client;

    // Now use the new connect_with_blob method
   ASSERT_TRUE(client.connect_to_server("127.0.0.1", port, *ds));  // Blob-enabled connect

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


}  // namespace limestone::testing
