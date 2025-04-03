#include "gtest/gtest.h"
#include "test_root.h"
#include <boost/process.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <signal.h>
#include <mutex>
#include "replication_test_helper.h"
#include "replication/replica_server.h"
#include "replication/replication_endpoint.h"
#include  <limestone/api/log_channel.h>

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::log_channel;
using limestone::api::datastore;

static constexpr const bool server_execute_as_thread = false;

static constexpr const char* base_location = "/tmp/scenario_test";
static constexpr const char* master_location = "/tmp/scenario_test/master";
static constexpr const char* replica_location = "/tmp/scenario_test/replica";

class scenario_test : public ::testing::Test {
protected:
    void SetUp() override {
        // prepare test directories
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(master_location);
        boost::filesystem::create_directories(replica_location);

        // start replica server
        uint16_t port = get_free_port();
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
        if (server_execute_as_thread) {
            int result = start_replica_as_thread();
            ASSERT_EQ(result, 0) << "Failed to start replica thread";
        } else {
            start_replica_as_process();
        }
    }

    void TearDown() override {
        // cleanup environment variable
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        // stop replica server
        stop_replica();

        // cleanup datastore
        ds.reset();

        // cleanup test directories
        boost::filesystem::remove_all(base_location);
    }

    void start_replica_as_process() {
        out_stream_ = std::make_unique<boost::process::ipstream>();
        err_stream_ = std::make_unique<boost::process::ipstream>();
    
        process_ = std::make_unique<boost::process::child>(
            "../src/tgreplica",
            replica_location,
            boost::process::std_out > *out_stream_,
            boost::process::std_err > *err_stream_);
    
        std::promise<void> initialized;
        std::future<void> wait_initialized = initialized.get_future();
    
        std::thread out_thread([&]() {
            std::string out_line;
            while (std::getline(*out_stream_, out_line)) {
                std::cout << "tgreplica> " << out_line << std::endl;
    
                if (out_line.find("initialized and listening") != std::string::npos) {
                    try {
                        initialized.set_value();
                    } catch (...) {
                    }
                }
            }
        });
    
        std::thread err_thread([&]() {
            std::string err_line;
            while (std::getline(*err_stream_, err_line)) {
                std::cerr << "tgreplica> " << err_line << std::endl;
            }
        });
    
        out_thread.detach();
        err_thread.detach();
    
        if (wait_initialized.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            FAIL() << "Timed out waiting for replica initialization";
        }
    }
    

    int start_replica_as_thread() {
        boost::filesystem::path log_dir_path(replica_location);
        limestone::replication::replication_endpoint endpoint{};
        
        server.initialize(log_dir_path);
            if (!server.start_listener(endpoint.get_sockaddr())) {
            return 1;
        }
        replica_thread_ = std::thread([this]() {
            server.accept_loop(); 
        });
        return 0;
    }

    void stop_replica() {
        if (server_execute_as_thread) {
            if (replica_thread_.joinable()) {
                server.shutdown(); 
                replica_thread_.join(); 
            }
        } else {
            if (process_ && process_->running()) {
                kill(process_->id(), SIGTERM);
                process_->wait();  
            }
        }
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(master_location);
        boost::filesystem::path metadata_location_path{master_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        ds = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &ds->create_channel(master_location);
        lc1_ = &ds->create_channel(master_location);
        ds->ready();
    }

    auto read_master_pwal00() {
        return read_log_file(master_location, "pwal_0000");
    }
    auto read_master_pwal01() {
        return read_log_file(master_location, "pwal_0001");
    }
    auto read_replica_pwal00() {
        return read_log_file(replica_location, "pwal_0000");
    }
    auto read_replica_pwal01() {
        return read_log_file(replica_location, "pwal_0001");
    }

protected:
    // fore replica server
    std::unique_ptr<boost::process::child> process_; 
    std::unique_ptr<boost::process::ipstream> out_stream_; 
    std::unique_ptr<boost::process::ipstream> err_stream_; 
    replica_server server{};
    std::thread replica_thread_;  

    // for master
    std::unique_ptr<api::datastore_test> ds;
    log_channel* lc0_{};
    log_channel* lc1_{};

};

TEST_F(scenario_test, test_process_running) {
    gen_datastore();
    ds->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();

    {
        auto master_entries = read_master_pwal00();
        ASSERT_EQ(master_entries.size(), 1);
        EXPECT_TRUE(AssertLogEntry(master_entries[0], 1, "k1", "v1", 1, 0, {}, log_entry::entry_type::normal_entry));

        auto replica_entries = read_replica_pwal00();
        ASSERT_EQ(replica_entries.size(), 1);
        EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1, "k1", "v1", 1, 0, {}, log_entry::entry_type::normal_entry));
    }



}

}  // namespace limestone::testing
