#include <limestone/api/log_channel.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <iostream>
#include <iterator>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"
#include "internal.h"
#include "replication/replica_server.h"
#include "replication/replication_endpoint.h"
#include "replication_test_helper.h"
#include "test_root.h"
#include <limestone/api/storage_id_type.h>

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::log_channel;
using limestone::api::datastore;
using limestone::internal::epoch_file_name;
using limestone::internal::last_durable_epoch;
using limestone::api::storage_id_type;

static constexpr const bool server_execute_as_thread = false;

static constexpr const char* base_location = "/tmp/scenario_test";
static constexpr const char* master_location = "/tmp/scenario_test/master";
static constexpr const char* replica_location = "/tmp/scenario_test/replica";


struct snapshot_entry {
    std::string key;
    std::string value;
    storage_id_type storage_id;
};


class scenario_test : public ::testing::Test {
protected:
    void SetUp() override {
        pthread_setname_np(pthread_self(), "master_main");

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
        unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
        unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");

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
            pthread_setname_np(pthread_self(), "out_thread");
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
            pthread_setname_np(pthread_self(), "err_thread");
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
            pthread_setname_np(pthread_self(), "replica_main");
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

    void gen_datastore(const char* location) {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location_path{location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        ds = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &ds->create_channel(location);
        lc1_ = &ds->create_channel(location);
        ds->ready();
    }

    auto get_snapshot_entries() {
        auto snapshot = ds->get_snapshot();
        auto cursor = snapshot->get_cursor();
        std::vector<snapshot_entry> snapshot_entries;
        while (cursor->next()) {
            snapshot_entry entry{};
            cursor->key(entry.key);
            cursor->value(entry.value);
            entry.storage_id = cursor->storage();
            snapshot_entries.emplace_back(entry);
        }
        return snapshot_entries;
    }

    auto read_master_pwal00() { return read_log_file(master_location, "pwal_0000"); }
    auto read_master_pwal01() { return read_log_file(master_location, "pwal_0001"); }
    auto read_replica_pwal00() { return read_log_file(replica_location, "pwal_0000"); }
    auto read_replica_pwal01() { return read_log_file(replica_location, "pwal_0001"); }
    auto get_master_epoch() {return get_epoch(master_location);}
    auto get_replica_epoch() {return get_epoch(replica_location);}
private:
    epoch_id_type get_epoch(boost::filesystem::path location) {
        auto epoch = last_durable_epoch(location / std::string(epoch_file_name));
        if (!epoch.has_value()) {
            return -1;
        }
        return epoch.value();
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

    // run test
    void run_minimal_test() {
        // Replica is already initialized in SetUp
        // Start the master
        gen_datastore(master_location);
        ds->switch_epoch(1);

        // Verify that PWAL is transferred to the replica
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

        // Verify that group commit is transferred
        ds->switch_epoch(2);
        EXPECT_EQ(get_master_epoch(), 1);
        EXPECT_EQ(get_replica_epoch(), 1);

        // Write PWAL in the next epoch
        lc0_->begin_session();
        lc0_->add_entry(1, "k2", "v2", {2, 0});
        lc0_->end_session();

        // Verify that writing PWAL alone does not advance the epoch
        EXPECT_EQ(get_master_epoch(), 1);
        EXPECT_EQ(get_replica_epoch(), 1);

        // Verify that the PWAL write is transferred to the replica
        {
            auto master_entries = read_master_pwal00();
            ASSERT_EQ(master_entries.size(), 2);
            EXPECT_TRUE(AssertLogEntry(master_entries[0], 1, "k1", "v1", 1, 0, {}, log_entry::entry_type::normal_entry));
            EXPECT_TRUE(AssertLogEntry(master_entries[1], 1, "k2", "v2", 2, 0, {}, log_entry::entry_type::normal_entry));

            auto replica_entries = read_replica_pwal00();
            ASSERT_EQ(replica_entries.size(), 2);
            EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1, "k1", "v1", 1, 0, {}, log_entry::entry_type::normal_entry));
            EXPECT_TRUE(AssertLogEntry(replica_entries[1], 1, "k2", "v2", 2, 0, {}, log_entry::entry_type::normal_entry));
        }

        // Verify that group commit is transferred
        ds->switch_epoch(3);
        EXPECT_EQ(get_master_epoch(), 2);
        EXPECT_EQ(get_replica_epoch(), 2);

        // Stop the master
        ds.reset();

        // Stop the replica
        stop_replica();

        // Start the master without a replica
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        gen_datastore(master_location);

        // Verify the snapshot
        {
            auto snapshot_entries = get_snapshot_entries();
            ASSERT_EQ(snapshot_entries.size(), 2);
            EXPECT_EQ(snapshot_entries[0].key, "k1");
            EXPECT_EQ(snapshot_entries[0].value, "v1");
            EXPECT_EQ(snapshot_entries[0].storage_id, 1);
            EXPECT_EQ(snapshot_entries[1].key, "k2");
            EXPECT_EQ(snapshot_entries[1].value, "v2");
            EXPECT_EQ(snapshot_entries[1].storage_id, 1);
        }
        // Stop the master and restart it with the replica's data
        ds.reset();
        gen_datastore(replica_location);

        // Verify the snapshot again
        {
            auto snapshot_entries = get_snapshot_entries();
            ASSERT_EQ(snapshot_entries.size(), 2);
            EXPECT_EQ(snapshot_entries[0].key, "k1");
            EXPECT_EQ(snapshot_entries[0].value, "v1");
            EXPECT_EQ(snapshot_entries[0].storage_id, 1);
            EXPECT_EQ(snapshot_entries[1].key, "k2");
            EXPECT_EQ(snapshot_entries[1].value, "v2");
            EXPECT_EQ(snapshot_entries[1].storage_id, 1);
        }
    }
};

TEST_F(scenario_test, minimal_test_disabled_async) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "disabled", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "disabled", 1);
   run_minimal_test();
}

TEST_F(scenario_test, minimal_test_std_async) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "std_async", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "std_async", 1);
   run_minimal_test();
}

TEST_F(scenario_test, minimal_test_single_thread_async) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "single_thread_async", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "single_thread_async", 1);
   run_minimal_test();
}

TEST_F(scenario_test, minimal_test_boost_thread_pool_async) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "boost_thread_pool_async", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "boost_thread_pool_async", 1);
   run_minimal_test();
}


}  // namespace limestone::testing
