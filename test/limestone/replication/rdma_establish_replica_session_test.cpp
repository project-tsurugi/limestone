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
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <limestone/replication/rdma_daemon_process.h>
#include <rdma/handshake_client_base.h>
#include <rdma/rdma_factory.h>
#include <rdma/rdma_handshake_payload.h>
#include <rdma/rdma_receive_event.h>
#include <replication/log_channel_limits.h>
#include <replication/message_group_commit.h>
#include <replication/replica_server.h>
#include <replication/replication_message.h>
#include <replication/replication_message_io.h>

#include "replication_test_helper.h"

namespace limestone::testing {

namespace {

using limestone::replication::decode_response_payload;
using limestone::replication::encode;
using limestone::replication::handshake_client_base;
using limestone::replication::log_channel_slots_limit;
using limestone::replication::make_handshake_connector;
using limestone::replication::rdma_handshake_start_payload;
using limestone::replication::replica_server;
using limestone::replication::replication_protocol_version;

constexpr std::chrono::milliseconds master_operation_timeout{5000};
constexpr std::uint64_t service_id = 58U;
constexpr std::uint32_t slot_count = 4U;

// The vendor mock does not dereference the remote DMA address on the replica side,
// so the master role can hand out an arbitrary value in place of a real receiver.
constexpr std::uint64_t stub_master_dma_address = 0xCAFEU;

// Builds the start payload the master role sends; tests tweak individual fields.
[[nodiscard]] rdma_handshake_start_payload make_start_payload(std::uint16_t channel_count) {
    rdma_handshake_start_payload payload{};
    payload.protocol_version = replication_protocol_version;
    payload.slot_count = slot_count;
    payload.master_dma_address = stub_master_dma_address;
    payload.channel_count = channel_count;
    payload.control_channel_id = channel_count;
    return payload;
}

// Builds an RDMA data event carrying one serialized replication message, as the
// master-side control channel send path would produce it.
[[nodiscard]] limestone::replication::rdma_data_event make_control_frame(
        std::uint16_t channel_id, std::uint16_t sequence_number,
        limestone::replication::replication_message const& message) {
    limestone::replication::replication_message_io io{std::string{}};
    limestone::replication::replication_message::send(io, message);
    auto const payload = io.get_out_string();
    limestone::replication::rdma_data_event event{};
    event.header.version = limestone::replication::rdma_frame_current_version;
    event.header.flags = 0U;
    event.header.sequence_number = sequence_number;
    event.header.channel_id = channel_id;
    event.header.payload_size = static_cast<std::uint32_t>(payload.size());
    event.payload.assign(payload.begin(), payload.end());
    return event;
}

} // namespace

/**
 * @brief Replica-side establish_rdma_session coverage against two real
 *        rdma_handshaked daemons, with the master side driven by a hand-written
 *        connector using real handshake payloads.
 */
class rdma_establish_replica_session_test : public ::testing::Test {
protected:
    void SetUp() override {
        std::error_code ec;
        auto const      tmp_root = std::filesystem::temp_directory_path(ec);
        ASSERT_FALSE(ec) << "temp_directory_path failed: " << ec.message();
        auto        base = tmp_root / "rdma_establish_replica_session_test_XXXXXX";
        std::string templ = base.string();
        char* const dir = ::mkdtemp(templ.data());
        ASSERT_NE(dir, nullptr) << "mkdtemp failed for the test temp directory";
        temp_dir_ = dir;
        conn_info_path_ = (std::filesystem::path{temp_dir_} / "conn.info").string();
        server_socket_path_ = (std::filesystem::path{temp_dir_} / "server.sock").string();
        client_socket_path_ = (std::filesystem::path{temp_dir_} / "client.sock").string();
        log_dir_ = (std::filesystem::path{temp_dir_} / "log").string();
        std::filesystem::create_directories(log_dir_, ec);
        ASSERT_FALSE(ec) << "failed to create the replica log directory: " << ec.message();

        // The replica_server's internal datastore loads the replication configuration
        // from the environment, so these must be in place before initialize().
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", server_socket_path_.c_str(), 1);
        ::setenv("TSURUGI_REPLICATION_SERVICE_ID", "58", 1);
        ::setenv("REPLICATION_RDMA_SLOTS", "4", 1);
        ::unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    }

    void TearDown() override {
        ::unsetenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET");
        ::unsetenv("TSURUGI_REPLICATION_SERVICE_ID");
        ::unsetenv("REPLICATION_RDMA_SLOTS");
        client_.reset();
        server_.reset();
        if (!temp_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(temp_dir_, ec);
        }
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

    /**
     * @brief Results the master role records while driving one handshake.
     */
    struct master_results {
        handshake_client_base::operation_result start;
        handshake_client_base::receive_result   response;
        handshake_client_base::operation_result finalize;
        handshake_client_base::operation_result ready;
        handshake_client_base::operation_result completion;
    };

    // Drives the master side of one handshake on the calling thread: sends the start
    // payload, receives the response, and (only when run_tail is set) completes the
    // finalize / ready / completion tail. Rejection tests stop after the response.
    void drive_master(std::vector<std::uint8_t> const& start_payload_bytes, bool run_tail,
        master_results& out) {
        auto connector_result =
            make_handshake_connector(client_socket_path_, master_operation_timeout);
        if (!connector_result.status.success) {
            out.start = {false, connector_result.status.error_message};
            return;
        }
        auto connector = std::move(connector_result.instance);
        out.start = connector->start(service_id, start_payload_bytes);
        if (!out.start.success) {
            return;
        }
        out.response = connector->receive_response();
        if (!out.response.success || !run_tail) {
            return;
        }
        out.finalize = connector->send_finalize({});
        if (!out.finalize.success) {
            return;
        }
        out.ready = connector->send_ready();
        if (!out.ready.success) {
            return;
        }
        out.completion = connector->receive_completion();
    }

    // Runs the replica establishment on a worker thread; it parks in wait_for_start
    // (unbounded) until the master role starts or unblock_replica() kills the daemons.
    [[nodiscard]] std::thread spawn_replica(replica_server& server, bool& established) {
        return std::thread([this, &server, &established] {
            established = server.establish_rdma_session(server_socket_path_, service_id);
        });
    }

    // There is no master-side retry (the replica must be seated first), so tests wait
    // until the replica's wait_for_start registration reaches the server daemon before
    // driving the master side.
    [[nodiscard]] bool wait_replica_seated() {
        return server_->wait_for_log("registering session (a_await_start)");
    }

    // Terminates both daemons so a replica parked on any handshake step gets unblocked
    // and the thread join cannot hang.
    void unblock_replica() {
        server_->terminate();
        client_->terminate();
    }

    std::string temp_dir_;
    std::string conn_info_path_;
    std::string server_socket_path_;
    std::string client_socket_path_;
    std::string log_dir_;

    std::unique_ptr<daemon_process> server_;
    std::unique_ptr<daemon_process> client_;

    static constexpr char const* handshake_timeout_arg = "--handshake-timeout=2000";
};

TEST_F(rdma_establish_replica_session_test, establish_succeeds_and_registers_channels) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    // No ASSERT is allowed between here and the join below: a fatal assertion would
    // return with the thread still joinable and std::terminate the whole binary.
    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        drive_master(encode(make_start_payload(2U)), true, master);
    }
    if (!seated || !master.completion.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.start.success) << master.start.error_message;
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    ASSERT_TRUE(master.finalize.success) << master.finalize.error_message;
    ASSERT_TRUE(master.ready.success) << master.ready.error_message;
    ASSERT_TRUE(master.completion.success) << master.completion.error_message;
    ASSERT_TRUE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_TRUE(response->accepted) << response->error_message;
    auto const dma_address = server.get_rdma_dma_address();
    ASSERT_TRUE(dma_address.has_value());
    EXPECT_EQ(response->replica_dma_address, dma_address.value());

    EXPECT_NE(server.get_rdma_log_channel_receiver(0U), nullptr);
    EXPECT_NE(server.get_rdma_log_channel_receiver(1U), nullptr);
    EXPECT_EQ(server.get_rdma_log_channel_receiver(2U), nullptr);

    // Control channel behavior over the established session. Verified here rather
    // than in separate tests because every establishment costs vendor-mock endpoint
    // slots (pid-keyed, max 64 per shm epoch, never freed) via the two daemon
    // processes. channel_count=2 puts the control channel on id 2, and the handler
    // runs on the calling thread, so the epoch is persisted when the call returns.
    limestone::replication::message_group_commit first_commit{7U};
    server.handle_rdma_data_event(make_control_frame(2U, 0U, first_commit));
    EXPECT_EQ(get_epoch(boost::filesystem::path{log_dir_}), 7U);

    limestone::replication::message_group_commit second_commit{9U};
    server.handle_rdma_data_event(make_control_frame(2U, 1U, second_commit));
    EXPECT_EQ(get_epoch(boost::filesystem::path{log_dir_}), 9U);

    // An unexpected message type is now FATAL, so this test no longer exercises the
    // former drop behaviour. The FATAL branch is covered by an EXPECT_DEATH in
    // replica_server_test: every establishment consumes vendor-mock endpoint slots,
    // so death cases belong on the daemon-free unit-test side.
    limestone::replication::message_group_commit third_commit{11U};
    server.handle_rdma_data_event(make_control_frame(2U, 2U, third_commit));
    EXPECT_EQ(get_epoch(boost::filesystem::path{log_dir_}), 11U);
}

TEST_F(rdma_establish_replica_session_test, establish_rejects_unsupported_protocol_version) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        auto payload = make_start_payload(1U);
        payload.protocol_version = replication_protocol_version + 1U;
        drive_master(encode(payload), false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    EXPECT_FALSE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_FALSE(response->accepted);
    EXPECT_NE(response->error_message.find("protocol version"), std::string::npos)
        << response->error_message;
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

TEST_F(rdma_establish_replica_session_test, establish_rejects_excessive_channel_count) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        auto const over_limit = static_cast<std::uint16_t>(log_channel_slots_limit + 1U);
        drive_master(encode(make_start_payload(over_limit)), false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    EXPECT_FALSE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_FALSE(response->accepted);
    EXPECT_NE(response->error_message.find("out of range"), std::string::npos)
        << response->error_message;
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

TEST_F(rdma_establish_replica_session_test, establish_rejects_zero_channel_count) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        drive_master(encode(make_start_payload(0U)), false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    EXPECT_FALSE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_FALSE(response->accepted);
    EXPECT_NE(response->error_message.find("out of range"), std::string::npos)
        << response->error_message;
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

TEST_F(rdma_establish_replica_session_test, establish_rejects_control_channel_id_overlap) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        auto payload = make_start_payload(2U);
        payload.control_channel_id = 0U; // overlaps data channel id 0
        drive_master(encode(payload), false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    EXPECT_FALSE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_FALSE(response->accepted);
    EXPECT_NE(response->error_message.find("overlaps"), std::string::npos)
        << response->error_message;
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

TEST_F(rdma_establish_replica_session_test, establish_releases_stack_on_master_death) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        // Valid start payload, but the master drops its connector right after the
        // response instead of sending finalize (run_tail=false destroys it on return).
        // The daemons relay the teardown as a session close, so the replica fails
        // without killing the daemons. Killing them mid-session must be avoided: it
        // leaves the fixed-name vendor-mock shared memory (/dev/shm/GnMock_*) in a
        // mid-transfer state that breaks the next daemon pair's bring-up.
        drive_master(encode(make_start_payload(1U)), false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;

    // The accepted response proves the replica had initialized its RDMA stack;
    // the failed establishment must have released it again.
    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_TRUE(response->accepted) << response->error_message;
    EXPECT_FALSE(established);
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

TEST_F(rdma_establish_replica_session_test, establish_rejects_malformed_start_payload) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());
    replica_server server{};
    server.initialize(boost::filesystem::path{log_dir_});

    bool established = false;
    auto replica_thread = spawn_replica(server, established);

    bool const seated = wait_replica_seated();
    master_results master{};
    if (seated) {
        drive_master(std::vector<std::uint8_t>{0x01U, 0x02U}, false, master);
    }
    if (!seated || !master.response.success) {
        unblock_replica();
    }
    replica_thread.join();

    ASSERT_TRUE(seated) << "replica registration did not reach the server daemon";
    ASSERT_TRUE(master.response.success) << master.response.error_message;
    EXPECT_FALSE(established);

    auto const response = decode_response_payload(master.response.payload);
    ASSERT_TRUE(response.has_value()) << "master received a malformed response payload";
    EXPECT_FALSE(response->accepted);
    EXPECT_NE(response->error_message.find("malformed"), std::string::npos)
        << response->error_message;
    EXPECT_FALSE(server.get_rdma_dma_address().has_value());
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
