/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifdef LIMESTONE_ENABLE_RDMA

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/filesystem.hpp>

#include <datastore_impl.h>
#include <log_channel_impl.h>
#include <limestone/api/log_channel.h>
#include <limestone/replication/rdma_daemon_process.h>
#include <limestone/replication/replication_test_helper.h>
#include <replication/replica_server.h>
#include <test_root.h>

namespace limestone::testing {

using limestone::api::log_channel;

namespace {

constexpr char const* base_location = "/tmp/scenario_tcpless_rdma_test";
constexpr char const* master_location = "/tmp/scenario_tcpless_rdma_test/master";
constexpr char const* replica_location = "/tmp/scenario_tcpless_rdma_test/replica";

constexpr std::uint64_t service_id = 59U;

/**
 * @brief Collects the inodes of all sockets held by the process from the
 *        "socket:[<inode>]" symlink targets under /proc/<pid>/fd.
 * @param pid Target process.
 * @return Set of socket inodes; empty when the fd directory itself is unreadable.
 */
std::set<std::string> read_socket_inodes(pid_t pid) {
    std::set<std::string> inodes;
    boost::filesystem::path const fd_dir{"/proc/" + std::to_string(pid) + "/fd"};
    boost::system::error_code iter_ec;
    // Use the error_code overloads so racing with process exit does not abort
    // the test (liveness checking is the caller's responsibility).
    boost::filesystem::directory_iterator it{fd_dir, iter_ec};
    for (; !iter_ec && it != boost::filesystem::directory_iterator{}; it.increment(iter_ec)) {
        boost::system::error_code ec;
        auto const target = boost::filesystem::read_symlink(it->path(), ec).string();
        // Ignore fds that cannot be read due to close races etc.
        if (ec) {
            continue;
        }
        constexpr char const* prefix = "socket:[";
        if (target.rfind(prefix, 0) == 0 && target.back() == ']') {
            inodes.insert(target.substr(std::strlen(prefix),
                target.size() - std::strlen(prefix) - 1));
        }
    }
    return inodes;
}

/**
 * @brief Returns whether /proc/<pid>/fd is readable and has at least one entry.
 * @param pid Target process.
 * @return true when at least one fd is visible.
 */
bool process_fds_visible(pid_t pid) {
    boost::system::error_code ec;
    boost::filesystem::directory_iterator const it{
        boost::filesystem::path{"/proc/" + std::to_string(pid) + "/fd"}, ec};
    return !ec && it != boost::filesystem::directory_iterator{};
}

/**
 * @brief Collects the inodes of all TCP sockets in the network namespace from
 *        /proc/<pid>/net/tcp and tcp6.
 * @param pid Process that selects the namespace to inspect.
 * @return Set of TCP socket inodes.
 *
 * After the header line each row reads
 * "sl local rem st tx:rx tr:tm retrnsmt uid timeout inode ...", so the inode is
 * the 10th field.
 */
std::set<std::string> read_tcp_inodes_in_namespace(pid_t pid) {
    std::set<std::string> inodes;
    for (char const* table : {"tcp", "tcp6"}) {
        std::ifstream in{"/proc/" + std::to_string(pid) + "/net/" + table};
        if (!in) {
            // A silent open failure would turn the check into a vacuous PASS, so
            // fail here; tcp6 is tolerated because it does not exist when IPv6
            // is disabled.
            if (std::string{table} == "tcp") {
                ADD_FAILURE() << "failed to open /proc/" << pid << "/net/" << table;
            }
            continue;
        }
        std::string line;
        std::getline(in, line);  // discard the header line
        while (std::getline(in, line)) {
            std::istringstream fields{line};
            std::string field;
            int read_count = 0;
            while (read_count < 10 && (fields >> field)) {
                ++read_count;
            }
            // Extraction failure at eof leaves field untouched, so accept only
            // rows with 10 fields read (shorter rows would retain a previous
            // column's value).
            if (read_count == 10) {
                inodes.insert(field);
            }
        }
    }
    return inodes;
}

/**
 * @brief Returns the inodes of the TCP sockets held by the process.
 * @param pid Target process.
 * @return Set of TCP socket inodes owned by the process.
 *
 * /proc/<pid>/net/tcp is a namespace-wide table that cannot be filtered by pid,
 * so ownership is determined by matching it against the socket inodes under
 * /proc/<pid>/fd.
 */
std::set<std::string> tcp_sockets_owned_by(pid_t pid) {
    auto const socket_inodes = read_socket_inodes(pid);
    auto const tcp_inodes = read_tcp_inodes_in_namespace(pid);
    std::set<std::string> owned;
    for (auto const& inode : socket_inodes) {
        if (tcp_inodes.find(inode) != tcp_inodes.end()) {
            owned.insert(inode);
        }
    }
    return owned;
}

/**
 * @brief Verifies that the process has opened no TCP socket since the baseline
 *        was taken.
 * @param pid Process to verify.
 * @param baseline TCP socket inodes owned when the measurement started.
 * @param label Process name used in failure messages.
 *
 * Earlier tests in the same binary may leave sockets behind, and those are also
 * inherited by the forked tgreplica, so the check uses the difference from the
 * baseline instead of an absolute count of zero. An exited or zombie process
 * exposes no fds and would make the check vacuously pass, so fd visibility is
 * checked first.
 */
void expect_no_new_tcp_sockets(pid_t pid, std::set<std::string> const& baseline,
        char const* label) {
    if (!process_fds_visible(pid)) {
        ADD_FAILURE() << label << " (pid " << pid
            << ") has no visible fds; the process may have exited";
        return;
    }
    std::vector<std::string> added;
    for (auto const& inode : tcp_sockets_owned_by(pid)) {
        if (baseline.find(inode) == baseline.end()) {
            added.push_back(inode);
        }
    }
    EXPECT_TRUE(added.empty()) << label << " (pid " << pid << ") opened "
        << added.size() << " TCP socket(s); the RDMA replication mode must not open TCP";
}

} // namespace

/**
 * @brief End-to-end coverage of the TCP-less RDMA replication path: a master datastore
 *        and a replica establish the session via two real rdma_handshaked daemons, and
 *        both the WAL data and the group commit epoch flow over RDMA with no TCP
 *        connection between them.
 *
 * The replica side runs in two forms: a real tgreplica process (process) and a
 * replica_server built in this same process (thread).
 */
class scenario_tcpless_rdma_test : public ::testing::Test {
protected:
    enum class replica_mode { process, thread };
    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(master_location);
        boost::filesystem::create_directories(replica_location);
        conn_info_path_ = std::string{base_location} + "/conn.info";
        server_socket_path_ = std::string{base_location} + "/server.sock";
        client_socket_path_ = std::string{base_location} + "/client.sock";

        ::setenv("TSURUGI_REPLICATION_SERVICE_ID", std::to_string(service_id).c_str(), 1);
        ::setenv("REPLICATION_RDMA_SLOTS", "1024", 1);
        ::unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    }

    void TearDown() override {
        // On the success path the thread was joined when the establishment completed;
        // it is joinable here only after a mid-test failure. establish may wait
        // indefinitely in wait_for_start, so terminate the daemons to unblock it.
        // Stop the running thread before destroying any resources (ds_ onwards).
        if (replica_thread_.joinable()) {
            if (server_) {
                server_->terminate();
            }
            if (client_) {
                client_->terminate();
            }
            replica_thread_.join();
        }
        ds_.reset();
        if (server_impl_) {
            server_impl_->shutdown();
            server_impl_.reset();
        }
        replica_.reset();
        client_.reset();
        server_.reset();
        ::unsetenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET");
        ::unsetenv("TSURUGI_REPLICATION_SERVICE_ID");
        ::unsetenv("REPLICATION_RDMA_SLOTS");
        boost::filesystem::remove_all(base_location);
    }

    void start_daemons() {
        server_ = std::make_unique<daemon_process>(std::vector<std::string>{
            "--export-conn-info=" + conn_info_path_,
            "--rdma-port=0",
            "--listen=" + server_socket_path_,
            handshake_timeout_arg,
        });
        ASSERT_TRUE(server_->started());
        ASSERT_TRUE(server_->wait_for_log("exported connection-info"))
            << "server daemon did not export the connection-info file in time";

        client_ = std::make_unique<daemon_process>(std::vector<std::string>{
            "--import-conn-info=" + conn_info_path_,
            "--rdma-port=0",
            "--listen=" + client_socket_path_,
            handshake_timeout_arg,
        });
        ASSERT_TRUE(client_->started());

        ASSERT_TRUE(server_->wait_for_log(
            "listening for local applications on " + server_socket_path_))
            << "server daemon did not start listening in time";
        ASSERT_TRUE(client_->wait_for_log(
            "listening for local applications on " + client_socket_path_))
            << "client daemon did not start listening in time";
    }

    // Starts tgreplica against the server daemon and waits until its wait_for_start
    // registration reaches the daemon: there is no master-side retry, so the master
    // must not start the handshake before the replica is seated.
    void start_replica_process() {
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", server_socket_path_.c_str(), 1);
        replica_ = std::make_unique<daemon_process>(
            std::vector<std::string>{replica_location}, TGREPLICA_BIN);
        ASSERT_TRUE(replica_->started());
        ASSERT_TRUE(replica_->wait_for_log("waiting for the master"))
            << "tgreplica did not reach the handshake wait in time";
        ASSERT_TRUE(server_->wait_for_log("registering session (a_await_start)"))
            << "replica registration did not reach the server daemon";
    }

    // Brings up a replica_server in this process, starts the establishment on a worker
    // thread, and waits until the replica is seated. The establishment itself completes
    // as the master side (gen_datastore) progresses, so the caller checks the result
    // with join_replica_thread() after the master is established.
    // An ASSERT between the thread start and the join would regress into
    // std::terminate with the thread still joinable on a fatal failure, so the
    // seating result is returned instead. On a seating failure the daemons are
    // terminated to unblock the indefinite wait_for_start before the join.
    [[nodiscard]] bool start_replica_in_thread() {
        // The replica-side datastore built by initialize() validates the replication
        // settings from the environment, so set the handshake socket first, just as
        // the process mode does before launching tgreplica. gen_datastore() later
        // overwrites it with the client-side value for the master.
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", server_socket_path_.c_str(), 1);
        server_impl_ = std::make_unique<limestone::replication::replica_server>();
        server_impl_->initialize(boost::filesystem::path{replica_location});
        replica_thread_ = std::thread([this]() {
            replica_established_ = server_impl_->establish_rdma_session(
                server_socket_path_, service_id);
        });
        bool const seated =
            server_->wait_for_log("registering session (a_await_start)");
        if (!seated) {
            server_->terminate();
            client_->terminate();
            replica_thread_.join();
        }
        return seated;
    }

    void join_replica_thread() {
        replica_thread_.join();
        ASSERT_TRUE(replica_established_) << "in-process replica failed to establish the session";
    }

    // Verifies that no TCP socket was opened since the baseline. Thread mode inspects
    // only this process (where master and replica coexist); process mode inspects
    // this process and tgreplica.
    void expect_tcpless(replica_mode mode, std::set<std::string> const& baseline) {
        expect_no_new_tcp_sockets(::getpid(), baseline,
            mode == replica_mode::process ? "master" : "master+replica");
        if (mode == replica_mode::process) {
            expect_no_new_tcp_sockets(replica_->pid(), baseline, "tgreplica");
        }
    }

    void run_wal_data_flow(replica_mode mode);

    void gen_datastore() {
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", client_socket_path_.c_str(), 1);
        limestone::api::configuration conf{};
        conf.set_data_location(master_location);
        ds_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &ds_->create_channel();
        lc1_ = &ds_->create_channel();
        ds_->ready();
    }

    auto read_replica_pwal00() { return read_log_file(replica_location, "pwal_0000"); }
    auto read_replica_pwal01() { return read_log_file(replica_location, "pwal_0001"); }

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

    std::string conn_info_path_;
    std::string server_socket_path_;
    std::string client_socket_path_;

    std::unique_ptr<daemon_process> server_;
    std::unique_ptr<daemon_process> client_;
    std::unique_ptr<daemon_process> replica_;

    std::unique_ptr<limestone::replication::replica_server> server_impl_;
    std::thread replica_thread_;
    bool replica_established_{false};

    std::unique_ptr<limestone::api::datastore_test> ds_;
    log_channel* lc0_{};
    log_channel* lc1_{};

    static constexpr char const* handshake_timeout_arg = "--handshake-timeout=2000";
};

// Verifies that tcp_sockets_owned_by() actually detects sockets rather than
// always returning empty. Without this, the TCP-less check in
// wal_data_flows_without_tcp could pass even with a broken parser. Earlier
// tests in the same binary may leave sockets behind, so the check uses diffs.
TEST_F(scenario_tcpless_rdma_test, tcp_socket_check_detects_listening_socket) {
    auto const baseline = tcp_sockets_owned_by(::getpid());

    int const fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // let the kernel pick a free port
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        int const bind_errno = errno;  // save errno before close() can overwrite it
        ::close(fd);
        FAIL() << "bind failed: " << strerror(bind_errno);
    }
    if (::listen(fd, 1) != 0) {
        int const listen_errno = errno;  // save errno before close() can overwrite it
        ::close(fd);
        FAIL() << "listen failed: " << strerror(listen_errno);
    }

    EXPECT_EQ(tcp_sockets_owned_by(::getpid()).size(), baseline.size() + 1);

    ::close(fd);
    EXPECT_EQ(tcp_sockets_owned_by(::getpid()).size(), baseline.size());
}

// Common body of wal_data_flows_without_tcp{,_thread_replica}; only the replica's
// execution form differs.
void scenario_tcpless_rdma_test::run_wal_data_flow(replica_mode mode) {
    // Take the pre-test ownership so TCP opened by the replication path shows
    // up as a diff. tgreplica is forked from this process, so inherited fds are
    // covered by the same baseline.
    auto const tcp_baseline = tcp_sockets_owned_by(::getpid());

    ASSERT_NO_FATAL_FAILURE(start_daemons());
    if (mode == replica_mode::process) {
        ASSERT_NO_FATAL_FAILURE(start_replica_process());
    } else {
        ASSERT_TRUE(start_replica_in_thread())
            << "replica registration did not reach the server daemon";
    }

    // ready() aborts via LOG_LP(FATAL) when the establishment fails, so returning
    // from gen_datastore() already implies the session is established.
    ASSERT_NO_FATAL_FAILURE(gen_datastore());
    if (mode == replica_mode::process) {
        ASSERT_TRUE(replica_->wait_for_log("initialized and listening"))
            << "tgreplica did not finish the session establishment in time";
    } else {
        ASSERT_NO_FATAL_FAILURE(join_replica_thread());
    }

    EXPECT_TRUE(ds_->get_impl()->is_rdma_enabled());
    EXPECT_NE(ds_->get_impl()->get_rdma_control_send_stream(), nullptr);
    EXPECT_TRUE(lc0_->get_impl()->has_rdma_send_stream());
    EXPECT_TRUE(lc1_->get_impl()->has_rdma_send_stream());

    // Verify that the session establishment opened no TCP connection
    expect_tcpless(mode, tcp_baseline);

    ds_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();

    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v2", {1, 0});
    lc1_->end_session();

    // end_session() waits for the RDMA ACK, which the replica returns after its
    // handler has written the entry, so the data is durable on the replica here.
    // wait_until() is kept as a guard against scheduling jitter.
    wait_until([this]() { return read_replica_pwal00().size() == 1; });
    wait_until([this]() { return read_replica_pwal01().size() == 1; });

    auto replica_entries0 = read_replica_pwal00();
    ASSERT_EQ(replica_entries0.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(replica_entries0[0], 1, "k1", "v1", 1, 0, {},
        log_entry::entry_type::normal_entry));

    auto replica_entries1 = read_replica_pwal01();
    ASSERT_EQ(replica_entries1.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(replica_entries1[0], 1, "k2", "v2", 1, 0, {},
        log_entry::entry_type::normal_entry));

    // Epoch 1 becomes durable when the epoch switches away from it. The group commit
    // flows over the RDMA control channel, and its ACK wait maps onto flush(), so the
    // replica-side epoch is already persisted when switch_epoch() returns.
    ds_->switch_epoch(2);
    EXPECT_EQ(get_epoch(master_location), 1U);
    EXPECT_EQ(get_epoch(replica_location), 1U);

    // A second round exercises consecutive control frames (sequence numbers advance).
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v3", {2, 0});
    lc0_->end_session();

    ds_->switch_epoch(3);
    EXPECT_EQ(get_epoch(master_location), 2U);
    EXPECT_EQ(get_epoch(replica_location), 2U);

    // Verify the processes are still TCP-less after WAL data and group commits
    expect_tcpless(mode, tcp_baseline);
}

TEST_F(scenario_tcpless_rdma_test, wal_data_flows_without_tcp) {
    run_wal_data_flow(replica_mode::process);
}

TEST_F(scenario_tcpless_rdma_test, wal_data_flows_without_tcp_thread_replica) {
    run_wal_data_flow(replica_mode::thread);
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
