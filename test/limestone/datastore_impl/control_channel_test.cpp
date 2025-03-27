#include <gtest/gtest.h>
#include <cstdlib>
#include <thread>
#include <arpa/inet.h>
#include "datastore_impl.h"
#include "replication/replica_server.h"
#include "replication/replica_connector.h"

namespace limestone::api {

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

class control_channel_test : public ::testing::Test {
protected:
    boost::filesystem::path location_{"/tmp/replica_server_test"};

    void SetUp() override {
        FLAGS_v = 70;
        boost::filesystem::remove_all(location_);
        boost::filesystem::create_directories(location_);
        
        uint16_t port = get_free_port();
        start_replica_server(port);  // Start the server during SetUp
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
    }

    void TearDown() override {
        stop_replica_server();  // Stop the server during TearDown
        boost::filesystem::remove_all(location_);
    }

    void start_replica_server(uint16_t port) {
        // Start the replica server in a separate thread
        server_.initialize(location_);

        auto addr = make_listen_addr(port);
        ASSERT_TRUE(server_.start_listener(addr));

        server_thread_ = std::make_unique<std::thread>([this, port]() {
            server_.accept_loop();
        });
    }

    void stop_replica_server() {
        if (server_thread_ && server_thread_->joinable()) {
            server_.shutdown();
            server_thread_->join();
        }
    }

private:
    replication::replica_server server_;
    std::unique_ptr<std::thread> server_thread_;
};

TEST_F(control_channel_test, open_control_channel_success) {
    datastore_impl datastore;

    // Test open control channel
    EXPECT_TRUE(datastore.open_control_channel());
    EXPECT_TRUE(datastore.has_replica());  

    // Verify that the control channel has been initialized correctly
    auto control_channel = datastore.get_control_channel();
    EXPECT_NE(control_channel, nullptr);
}


TEST_F(control_channel_test, open_control_channel_failure_invalid_endpoint) {
    // Set an invalid endpoint
    setenv("TSURUGI_REPLICATION_ENDPOINT", "invalid://endpoint", 1);
    
    datastore_impl datastore;
    EXPECT_FALSE(datastore.open_control_channel());
    EXPECT_FALSE(datastore.has_replica()); 

    // Verify that the control channel is not initialized (should be null)
    auto control_channel = datastore.get_control_channel();
    EXPECT_EQ(control_channel, nullptr); 
}

TEST_F(control_channel_test, open_control_channel_with_no_endpoint) {
    // Ensure no endpoint is set
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");

    datastore_impl datastore;
    EXPECT_FALSE(datastore.open_control_channel());
    EXPECT_FALSE(datastore.has_replica()); 

    // Verify that the control channel is not initialized (should be null)
    auto control_channel = datastore.get_control_channel();
    EXPECT_EQ(control_channel, nullptr); 
}

}  // namespace limestone::api
