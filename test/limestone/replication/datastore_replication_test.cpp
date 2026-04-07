#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <optional>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "blob_file_garbage_collector.h"
#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "log_channel_impl.h"
#include "log_entry.h"
#include "limestone/logging.h"
#include "replication/replica_server.h"
#include "replication_test_helper.h"
#include "test_root.h"
#include "rdma/rdma_send_stream_base.h"

namespace limestone::testing {

using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base = "/tmp/datastore_replication_test";
constexpr const char* master = "/tmp/datastore_replication_test/master";
constexpr const char* replica = "/tmp/datastore_replication_test/replica";

namespace {

struct rdma_param {
    std::string name;
    std::optional<uint32_t> rdma_slots;
};

inline std::ostream& operator<<(std::ostream& os, rdma_param const& param) {
    return os << param.name;
}

class fake_rdma_send_stream : public limestone::replication::rdma_send_stream_base {
public:
    [[nodiscard]] send_result send_bytes(std::vector<std::uint8_t> const&, std::size_t, std::size_t length) noexcept override {
        return { true, "", length };
    }

    [[nodiscard]] send_result send_all_bytes(std::vector<std::uint8_t> const&, std::size_t, std::size_t length) noexcept override {
        return { true, "", length };
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        return { true, "" };
    }
};

}  // namespace

class datastore_replication_test
    : public ::testing::Test
    , public ::testing::WithParamInterface<rdma_param> {
protected:
    std::unique_ptr<api::datastore_test> datastore_;

    log_channel* lc0_{};
    log_channel* lc1_{};

    void SetUp() override {
        auto param = GetParam();
        if (param.rdma_slots.has_value()) {
            setenv("REPLICATION_RDMA_SLOTS", std::to_string(param.rdma_slots.value()).c_str(), 1);
        } else {
            unsetenv("REPLICATION_RDMA_SLOTS");
        }

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

        uint16_t port = get_free_port();
        start_replica_server(port); 
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
    }

    void TearDown() override {
        unsetenv("REPLICATION_RDMA_SLOTS");
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        stop_replica_server(); 
        std::string cmd = "rm -rf " + std::string(base);;
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
    }

    void gen_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(master);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &datastore_->create_channel();
        lc1_ = &datastore_->create_channel();
    }

    void start_replica_server(uint16_t port) {
        // Start the replica server in a separate thread
        server_.initialize(boost::filesystem::path(replica));

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
        datastore_ = nullptr;
    }

    replication::replica_server& server_for_test() {
        return server_;
    }

private:
    replication::replica_server server_;
    std::unique_ptr<std::thread> server_thread_;
};

TEST_P(datastore_replication_test, open_control_channel_success) {
    datastore_impl datastore;

    EXPECT_TRUE(datastore.open_control_channel());
    EXPECT_TRUE(datastore.has_replica());

    auto control_channel = datastore.get_control_channel();
    EXPECT_NE(control_channel, nullptr);

    if (GetParam().rdma_slots.has_value()) {
        EXPECT_NE(datastore.get_rdma_sender(), nullptr);
        EXPECT_TRUE(server_for_test().get_rdma_dma_address().has_value());
    } else {
        EXPECT_EQ(datastore.get_rdma_sender(), nullptr);
    }
}

TEST_P(datastore_replication_test, open_control_channel_failure_invalid_endpoint) {
    // Set an invalid endpoint
    setenv("TSURUGI_REPLICATION_ENDPOINT", "invalid://endpoint", 1);
    
    datastore_impl datastore;
    EXPECT_FALSE(datastore.open_control_channel());
    EXPECT_FALSE(datastore.has_replica()); 

    // Verify that the control channel is not initialized (should be null)
    auto control_channel = datastore.get_control_channel();
    EXPECT_EQ(control_channel, nullptr); 
}

TEST_P(datastore_replication_test, open_control_channel_with_no_endpoint) {
    // Ensure no endpoint is set
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");

    datastore_impl datastore;
    EXPECT_FALSE(datastore.open_control_channel());
    EXPECT_FALSE(datastore.has_replica()); 

    // Verify that the control channel is not initialized (should be null)
    auto control_channel = datastore.get_control_channel();
    EXPECT_EQ(control_channel, nullptr); 
}

TEST_P(datastore_replication_test, open_control_channel_via_datastore_ready) {
    gen_datastore();
    EXPECT_EQ(datastore_->get_impl()->get_control_channel(), nullptr);
    datastore_->ready();
    EXPECT_NE(datastore_->get_impl()->get_control_channel(), nullptr);
    EXPECT_TRUE(datastore_->get_impl()->has_replica());

    EXPECT_NE(lc0_->get_impl()->get_replica_connector(), nullptr);
    EXPECT_NE(lc1_->get_impl()->get_replica_connector(), nullptr);
    datastore_->shutdown();
}


TEST_P(datastore_replication_test, not_open_control_channel_via_datastore_ready) {
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");

    gen_datastore();
    EXPECT_EQ(datastore_->get_impl()->get_control_channel(), nullptr);
    datastore_->ready();
    EXPECT_EQ(datastore_->get_impl()->get_control_channel(), nullptr);
    EXPECT_FALSE(datastore_->get_impl()->has_replica());

    EXPECT_EQ(lc0_->get_impl()->get_replica_connector(), nullptr);
    EXPECT_EQ(lc1_->get_impl()->get_replica_connector(), nullptr);
}

TEST_P(datastore_replication_test, fail_open_control_channel_via_datastore_ready) {
    // Set an invalid endpoint
    setenv("TSURUGI_REPLICATION_ENDPOINT", "invalid://endpoint", 1);

    gen_datastore();
    EXPECT_EQ(datastore_->get_impl()->get_control_channel(), nullptr);
    EXPECT_DEATH({
        datastore_->ready();
    }, "Failed to open replication control channel.");
}


TEST_P(datastore_replication_test, replica_death_before_create_log_channel) {
    stop_replica_server();
    EXPECT_DEATH({
        gen_datastore();
    }, "Failed to create log channel connector.");
}

#ifdef LIMESTONE_ENABLE_RDMA
INSTANTIATE_TEST_SUITE_P(
    rdma_toggle,
    datastore_replication_test,
    ::testing::Values(rdma_param{"tcp", std::nullopt}, rdma_param{"rdma_128", 128U}),
    [](const ::testing::TestParamInfo<rdma_param>& info) {
        return info.param.name;
    });
#else
INSTANTIATE_TEST_SUITE_P(
    rdma_toggle,
    datastore_replication_test,
    ::testing::Values(rdma_param{"tcp", std::nullopt}),
    [](const ::testing::TestParamInfo<rdma_param>& info) {
        return info.param.name;
    });
#endif // LIMESTONE_ENABLE_RDMA

}  // namespace limestone::testing
