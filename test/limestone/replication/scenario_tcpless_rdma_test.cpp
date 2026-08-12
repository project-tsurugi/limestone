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

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include <datastore_impl.h>
#include <log_channel_impl.h>
#include <limestone/api/log_channel.h>
#include <limestone/replication/rdma_daemon_process.h>
#include <limestone/replication/replication_test_helper.h>
#include <test_root.h>

namespace limestone::testing {

using limestone::api::log_channel;

namespace {

constexpr char const* base_location = "/tmp/scenario_tcpless_rdma_test";
constexpr char const* master_location = "/tmp/scenario_tcpless_rdma_test/master";
constexpr char const* replica_location = "/tmp/scenario_tcpless_rdma_test/replica";

} // namespace

/**
 * @brief End-to-end coverage of the TCP-less RDMA replication path: a master datastore
 *        and a real tgreplica process establish the session via two real rdma_handshaked
 *        daemons and WAL data flows over RDMA with no TCP connection between them.
 *
 * Group commit is not propagated in this transitional mode (replaced in phase 3), so the
 * test asserts data arrival only and leaves the replica epoch unverified.
 */
class scenario_tcpless_rdma_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(master_location);
        boost::filesystem::create_directories(replica_location);
        conn_info_path_ = std::string{base_location} + "/conn.info";
        server_socket_path_ = std::string{base_location} + "/server.sock";
        client_socket_path_ = std::string{base_location} + "/client.sock";

        ::setenv("TSURUGI_REPLICATION_SERVICE_ID", "59", 1);
        ::setenv("REPLICATION_RDMA_SLOTS", "1024", 1);
        ::unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    }

    void TearDown() override {
        ds_.reset();
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

    std::unique_ptr<limestone::api::datastore_test> ds_;
    log_channel* lc0_{};
    log_channel* lc1_{};

    static constexpr char const* handshake_timeout_arg = "--handshake-timeout=2000";
};

TEST_F(scenario_tcpless_rdma_test, wal_data_flows_without_tcp) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    ASSERT_NO_FATAL_FAILURE(start_replica_process());

    // ready() aborts via LOG_LP(FATAL) when the establishment fails, so returning
    // from gen_datastore() already implies the session is established.
    ASSERT_NO_FATAL_FAILURE(gen_datastore());
    ASSERT_TRUE(replica_->wait_for_log("initialized and listening"))
        << "tgreplica did not finish the session establishment in time";

    EXPECT_TRUE(ds_->get_impl()->is_rdma_enabled());
    EXPECT_NE(ds_->get_impl()->get_rdma_control_send_stream(), nullptr);
    EXPECT_TRUE(lc0_->get_impl()->has_rdma_send_stream());
    EXPECT_TRUE(lc1_->get_impl()->has_rdma_send_stream());

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
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
