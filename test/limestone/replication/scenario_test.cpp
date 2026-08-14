#include <limestone/api/log_channel.h>
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>

#include "gtest/gtest.h"
#include <datastore_impl.h>
#include <log_channel_impl.h>
#include "blob_file_resolver.h"
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

static constexpr const char* base_location = "/tmp/scenario_test";
static constexpr const char* master_location = "/tmp/scenario_test/master";
static constexpr const char* replica_location = "/tmp/scenario_test/replica";

// File-local: keep this struct out of external linkage to avoid ODR collision
// with similarly-shaped parametrization structs that other replication test
// files define in the same namespace.
namespace {

struct scenario_param {
    std::string name;
    std::optional<uint32_t> rdma_slots;
    bool execute_as_process;
};

inline std::ostream& operator<<(std::ostream& os, scenario_param const& param) {
    return os << param.name;
}

} // namespace


struct snapshot_entry {
    std::string key;
    std::string value;
    storage_id_type storage_id;
};

std::string read_file(boost::filesystem::path const& path) {
    std::ifstream ifs(path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}


class scenario_test : public ::testing::Test, public ::testing::WithParamInterface<scenario_param> {
protected:
    void SetUp() override {
        pthread_setname_np(pthread_self(), "master_main");

        // prepare test directories
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(master_location);
        boost::filesystem::create_directories(replica_location);

        auto param = GetParam();
        if (param.rdma_slots.has_value()) {
            setenv("REPLICATION_RDMA_SLOTS", std::to_string(param.rdma_slots.value()).c_str(), 1);
        } else {
            unsetenv("REPLICATION_RDMA_SLOTS");
        }
        setenv("GLOG_vmodule", "rdma_sender_detail=50,send_buffer_pool=50,log_channel=50", 1);
        setenv("GLOG_logtostderr", "1", 1);
        setenv("GLOG_logbufsecs", "0", 1);

        // start replica server
        uint16_t port = get_free_port();
        setenv("TSURUGI_REPLICATION_ENDPOINT", ("tcp://127.0.0.1:" + std::to_string(port)).c_str(), 1);
        if (param.execute_as_process) {
            start_replica_as_process();
        } else {
            int result = start_replica_as_thread();
            ASSERT_EQ(result, 0) << "Failed to start replica thread";
        }
    }

    void TearDown() override {
        // cleanup environment variable
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        unsetenv("REPLICATION_RDMA_SLOTS");
        unsetenv("GLOG_vmodule");
        unsetenv("GLOG_logtostderr");
        unsetenv("GLOG_logbufsecs");
        // stop replica server
        stop_replica();

        // cleanup datastore
        ds.reset();

        // cleanup test directories
        boost::filesystem::remove_all(base_location);
    }

    void start_replica_as_process() {
        int out_pipe[2]{-1, -1};
        int err_pipe[2]{-1, -1};
        if (pipe(out_pipe) != 0) {
            FAIL() << "Failed to create stdout pipe";
        }
        if (pipe(err_pipe) != 0) {
            ::close(out_pipe[0]);
            ::close(out_pipe[1]);
            FAIL() << "Failed to create stderr pipe";
        }

        process_pid_ = fork();
        ASSERT_NE(process_pid_, -1) << "Failed to fork replica process";

        if (process_pid_ == 0) {
            ::close(out_pipe[0]);
            ::close(err_pipe[0]);
            ::dup2(out_pipe[1], STDOUT_FILENO);
            ::dup2(err_pipe[1], STDERR_FILENO);
            ::close(out_pipe[1]);
            ::close(err_pipe[1]);

            execl("../src/tgreplica", "../src/tgreplica", replica_location, static_cast<char*>(nullptr));
            std::perror("execl");
            _exit(127);
        }

        ::close(out_pipe[1]);
        ::close(err_pipe[1]);
        out_stream_ = fdopen(out_pipe[0], "r");
        if (out_stream_ == nullptr) {
            ::close(out_pipe[0]);
            ::close(err_pipe[0]);
            FAIL() << "Failed to open stdout stream";
        }
        err_stream_ = fdopen(err_pipe[0], "r");
        if (err_stream_ == nullptr) {
            fclose(out_stream_);
            out_stream_ = nullptr;
            ::close(err_pipe[0]);
            FAIL() << "Failed to open stderr stream";
        }
        // The thread only uses this promise while start_replica_as_process() is blocked
        // on wait_initialized.wait_for(), so the reference does not outlive this scope.
        std::promise<void> initialized;
        std::future<void> wait_initialized = initialized.get_future();

        out_thread_ = std::thread([&initialized, this]() {
            pthread_setname_np(pthread_self(), "out_thread");
            std::string out_line;
            char* line = nullptr;
            size_t capacity = 0;
            while (::getline(&line, &capacity, out_stream_) != -1) {
                out_line.assign(line);
                if (!out_line.empty() && out_line.back() == '\n') {
                    out_line.pop_back();
                }
                std::cout << "tgreplica> " << out_line << std::endl;

                if (out_line.find("initialized and listening") != std::string::npos) {
                    try {
                        initialized.set_value();
                    } catch (...) {
                    }
                }
            }
            free(line);
        });

        err_thread_ = std::thread([this]() {
            pthread_setname_np(pthread_self(), "err_thread");
            std::string err_line;
            char* line = nullptr;
            size_t capacity = 0;
            while (::getline(&line, &capacity, err_stream_) != -1) {
                err_line.assign(line);
                if (!err_line.empty() && err_line.back() == '\n') {
                    err_line.pop_back();
                }
                std::cerr << "tgreplica> " << err_line << std::endl;
            }
            free(line);
        });

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
        if (! GetParam().execute_as_process) {
            if (replica_thread_.joinable()) {
                server.shutdown();
                replica_thread_.join();
            }
        } else {
            if (process_pid_ > 0) {
                if (kill(process_pid_, SIGTERM) != 0 && errno != ESRCH) {
                    int error_num = errno;
                    FAIL() << "Failed to terminate replica process: "
                           << std::strerror(error_num) << " (errno=" << error_num << ")";
                }
                waitpid(process_pid_, nullptr, 0);
                process_pid_ = -1;
            }
            if (out_thread_.joinable()) {
                out_thread_.join();
            }
            if (err_thread_.joinable()) {
                err_thread_.join();
            }
            if (out_stream_ != nullptr) {
                fclose(out_stream_);
                out_stream_ = nullptr;
            }
            if (err_stream_ != nullptr) {
                fclose(err_stream_);
                err_stream_ = nullptr;
            }
        }
    }

    void gen_datastore(const char* location) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);

        ds = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &ds->create_channel();
        lc1_ = &ds->create_channel();
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
    auto get_master_epoch() { return get_epoch(master_location); }
    auto get_replica_epoch() { return get_epoch(replica_location); }

    template<typename Predicate>
    void wait_until(Predicate predicate,
                    std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(predicate()) << "Timed out waiting for replication result";
    }

private:
    epoch_id_type get_epoch(boost::filesystem::path location) {
        auto epoch = last_durable_epoch(location / std::string(epoch_file_name));
        if (!epoch.has_value()) {
            return -1;
        }
        return epoch.value();
    }

protected:
    // for replica server
    pid_t process_pid_{-1};
    FILE* out_stream_{nullptr};
    FILE* err_stream_{nullptr};
    replica_server server{};
    std::thread replica_thread_;
    std::thread out_thread_;
    std::thread err_thread_;

    // for master
    std::unique_ptr<api::datastore_test> ds;
    log_channel* lc0_{};
    log_channel* lc1_{};
};

TEST_P(scenario_test, minimal_test) {
    // Replica is already initialized in SetUp
    // Start the master
    gen_datastore(master_location);

    if (GetParam().rdma_slots.has_value()) {
        EXPECT_TRUE(ds->get_impl()->is_rdma_enabled());
        EXPECT_NE(ds->get_impl()->get_rdma_sender(), nullptr);
        EXPECT_TRUE(lc0_->get_impl()->has_rdma_send_stream());
    }


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
    if (GetParam().rdma_slots.has_value()) {
        unsetenv("REPLICATION_RDMA_SLOTS");
    }
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

TEST_P(scenario_test, blob_replication_end_to_end) {
    gen_datastore(master_location);

    if (GetParam().rdma_slots.has_value()) {
        EXPECT_TRUE(ds->get_impl()->is_rdma_enabled());
        EXPECT_NE(ds->get_impl()->get_rdma_sender(), nullptr);
        EXPECT_TRUE(lc0_->get_impl()->has_rdma_send_stream());
    }

    constexpr blob_id_type blob_id = 9001U;
    std::string blob_content(8192, '\0');
    for (std::size_t i = 0; i < blob_content.size(); ++i) {
        blob_content[i] = static_cast<char>('a' + (i % 26));
    }

    auto master_blob_path = ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(master_blob_path.parent_path());
    {
        std::ofstream ofs(master_blob_path.string(), std::ios::binary);
        ofs.write(blob_content.data(), static_cast<std::streamsize>(blob_content.size()));
    }

    ds->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "blob-key", "blob-value", {1, 0}, {blob_id});
    lc0_->end_session();

    wait_until([this]() {
        auto replica_entries = read_replica_pwal00();
        return replica_entries.size() == 1;
    });

    auto replica_entries = read_replica_pwal00();
    ASSERT_EQ(replica_entries.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(
        replica_entries[0], 1, "blob-key", "blob-value", 1, 0, {blob_id},
        log_entry::entry_type::normal_with_blob));

    limestone::internal::blob_file_resolver replica_blob_resolver{
        boost::filesystem::path(replica_location)};
    auto replica_blob_path = replica_blob_resolver.resolve_path(blob_id);
    wait_until([&replica_blob_path]() {
        return boost::filesystem::exists(replica_blob_path);
    });
    EXPECT_EQ(read_file(replica_blob_path), blob_content);

    ds->switch_epoch(2);
    EXPECT_EQ(get_master_epoch(), 1);
    EXPECT_EQ(get_replica_epoch(), 1);
}

// Both thread and process modes run because either may surface distinct bugs
// (process-only catches wire/handshake issues; thread-only catches in-process
// replica_server lifecycle issues and provides a faster dev loop). The RDMA
// variants also run in both modes: the vendor mock's GnRdmaWrite / GnRdmaReceive
// instances are not process-wide singletons, so the master side (data send +
// ACK receive) and the replica side (data receive + ACK send) — four instances
// in total — can coexist in one process just like TCP.
#ifdef LIMESTONE_ENABLE_RDMA
INSTANTIATE_TEST_SUITE_P(
    variants,
    scenario_test,
    ::testing::Values(
        scenario_param{"tcp_thread", std::nullopt, false},
        scenario_param{"tcp_process", std::nullopt, true},
        scenario_param{"rdma_thread", 1024U, false},
        scenario_param{"rdma_process", 1024U, true}),
    [](const ::testing::TestParamInfo<scenario_param>& info) {
        return info.param.name;
    });
#else
INSTANTIATE_TEST_SUITE_P(
    variants,
    scenario_test,
    ::testing::Values(
        scenario_param{"tcp_thread", std::nullopt, false},
        scenario_param{"tcp_process", std::nullopt, true}),
    [](const ::testing::TestParamInfo<scenario_param>& info) {
        return info.param.name;
    });
#endif // LIMESTONE_ENABLE_RDMA

}  // namespace limestone::testing
