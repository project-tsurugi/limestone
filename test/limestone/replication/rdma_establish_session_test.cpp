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

#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <datastore_impl.h>
#include <log_channel_impl.h>
#include <limestone/replication/rdma_daemon_process.h>
#include <rdma/handshake_client_base.h>
#include <rdma/rdma_factory.h>
#include <rdma/rdma_handshake_payload.h>
#include <rdma/rdma_frame_buffer_base.h>
#include <rdma/rdma_send_stream_base.h>
#include <replication/message_group_commit.h>
#include <replication/replication_message.h>
#include <replication/replication_message_io.h>

#include "test_rdma_frame_buffer.h"

namespace limestone::testing {

namespace {

using limestone::api::datastore_impl;
using limestone::replication::decode_start_payload;
using limestone::replication::encode;
using limestone::replication::handshake_acceptor_base;
using limestone::replication::handshake_client_base;
using limestone::replication::make_handshake_acceptor;
using limestone::replication::rdma_handshake_response_payload;
using limestone::replication::replication_protocol_version;

constexpr std::chrono::milliseconds default_wait_timeout{5000};
constexpr std::uint64_t service_id = 57U;
constexpr std::uint32_t slot_count = 4U;

// The vendor mock does not dereference the remote DMA address on the master side,
// so the stub can hand out an arbitrary value in place of a real replica receiver.
constexpr std::uint64_t stub_replica_dma_address = 0xBEEFU;

/**
 * @brief Fake control channel send stream recording submitted frames and flush calls.
 */
class capturing_control_send_stream : public limestone::replication::rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<limestone::replication::rdma_frame_buffer_base>
    acquire_frame_buffer(std::size_t max_payload, std::size_t min_capacity) noexcept override {
        auto const capacity = std::max(granted_frame_capacity(max_payload), min_capacity);
        return std::make_unique<test_rdma_frame_buffer>(capacity);
    }

    [[nodiscard]] send_result submit_frame_buffer(
            limestone::replication::rdma_frame_buffer_base& frame,
            std::size_t payload_size) override {
        auto& test_frame = dynamic_cast<test_rdma_frame_buffer&>(frame);
        submitted_.emplace_back(test_frame.take_written(payload_size));
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        ++flush_count_;
        return {true, ""};
    }

    std::vector<std::vector<std::uint8_t>> submitted_{};
    std::size_t flush_count_{0};
};

} // namespace

/**
 * @brief Master-side establish_rdma_session coverage against two real rdma_handshaked
 *        daemons, with the replica side driven by a hand-written acceptor stub
 *        (replica_server takes over that role in the next step).
 */
class rdma_establish_session_test : public ::testing::Test {
protected:
    void SetUp() override {
        std::error_code ec;
        auto const      tmp_root = std::filesystem::temp_directory_path(ec);
        ASSERT_FALSE(ec) << "temp_directory_path failed: " << ec.message();
        auto        base = tmp_root / "rdma_establish_session_test_XXXXXX";
        std::string templ = base.string();
        char* const dir = ::mkdtemp(templ.data());
        ASSERT_NE(dir, nullptr) << "mkdtemp failed for the test temp directory";
        temp_dir_ = dir;
        conn_info_path_ = (std::filesystem::path{temp_dir_} / "conn.info").string();
        server_socket_path_ = (std::filesystem::path{temp_dir_} / "server.sock").string();
        client_socket_path_ = (std::filesystem::path{temp_dir_} / "client.sock").string();

        // The datastore_impl constructor loads the replication configuration from the
        // environment, so these must be in place before each test constructs one.
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", client_socket_path_.c_str(), 1);
        ::setenv("TSURUGI_REPLICATION_SERVICE_ID", "57", 1);
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
     * @brief Results the acceptor stub records while serving the replica side of
     *        one handshake.
     */
    struct stub_results {
        std::unique_ptr<handshake_acceptor_base> acceptor;
        handshake_client_base::receive_result    start;
        handshake_client_base::operation_result  response;
        handshake_client_base::receive_result    finalize;
        handshake_client_base::operation_result  complete;
    };

    // Serves one accepted handshake on a worker thread: accepts the start payload,
    // returns an accepted response with the stub DMA address, then completes the
    // finalize / ready / completion tail.
    [[nodiscard]] std::thread spawn_acceptor_stub(stub_results& out) {
        return std::thread([this, &out] {
            auto acceptor_result =
                make_handshake_acceptor(server_socket_path_, default_wait_timeout);
            if (!acceptor_result.status.success) {
                out.start = {false, acceptor_result.status.error_message, {}};
                return;
            }
            out.acceptor = std::move(acceptor_result.instance);
            out.start = out.acceptor->wait_for_start(service_id);
            if (!out.start.success) {
                return;
            }
            rdma_handshake_response_payload response{};
            response.accepted = true;
            response.replica_dma_address = stub_replica_dma_address;
            out.response = out.acceptor->send_response(encode(response));
            if (!out.response.success) {
                return;
            }
            out.finalize = out.acceptor->receive_finalize();
            if (!out.finalize.success) {
                return;
            }
            out.complete = out.acceptor->complete();
        });
    }

    // There is no master-side retry (the replica must be seated first), so tests wait
    // until the stub's wait_for_start registration reaches the server daemon before
    // starting the master side.
    [[nodiscard]] bool wait_acceptor_seated() {
        return server_->wait_for_log("registering session (a_await_start)");
    }

    // Terminates both daemons so a stub parked on any handshake step gets unblocked
    // and the thread join cannot hang.
    void unblock_stub() {
        server_->terminate();
        client_->terminate();
    }

    std::string temp_dir_;
    std::string conn_info_path_;
    std::string server_socket_path_;
    std::string client_socket_path_;

    std::unique_ptr<daemon_process> server_;
    std::unique_ptr<daemon_process> client_;

    static constexpr char const* handshake_timeout_arg = "--handshake-timeout=2000";
};

TEST_F(rdma_establish_session_test, establish_fails_without_log_channels) {
    // A channel count of zero is rejected before the handshake starts, so no
    // daemon is involved.
    datastore_impl impl{};
    EXPECT_FALSE(impl.establish_rdma_session());
    EXPECT_EQ(impl.get_rdma_sender(), nullptr);
    EXPECT_EQ(impl.get_rdma_control_send_stream(), nullptr);
    EXPECT_FALSE(impl.has_replica());
}

TEST_F(rdma_establish_session_test, ready_establishes_session_with_log_channels) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());

    std::filesystem::path const location = std::filesystem::path{temp_dir_} / "log";
    limestone::api::configuration conf{};
    conf.set_data_location(location);
    auto ds = std::make_unique<limestone::api::datastore>(conf);
    auto& channel0 = ds->create_channel();
    auto& channel1 = ds->create_channel();

    stub_results stub{};
    auto acceptor_thread = spawn_acceptor_stub(stub);

    bool const seated = wait_acceptor_seated();
    if (seated) {
        // ready() aborts via LOG_LP(FATAL) when the establishment fails, so reaching
        // the join below already implies success.
        ds->ready();
    } else {
        unblock_stub();
    }
    acceptor_thread.join();

    ASSERT_TRUE(seated) << "acceptor registration did not reach the server daemon";
    ASSERT_TRUE(stub.start.success) << stub.start.error_message;
    ASSERT_TRUE(stub.complete.success) << stub.complete.error_message;

    auto const start = decode_start_payload(stub.start.payload);
    ASSERT_TRUE(start.has_value()) << "stub received a malformed start payload";
    EXPECT_EQ(start->slot_count, slot_count);
    EXPECT_EQ(start->channel_count, 2U);
    EXPECT_EQ(start->control_channel_id, 2U);

    auto* impl = ds->get_impl();
    ASSERT_NE(impl, nullptr);
    EXPECT_NE(impl->get_rdma_control_send_stream(), nullptr);
    EXPECT_TRUE(impl->has_replica());
    EXPECT_TRUE(channel0.get_impl()->has_rdma_send_stream());
    EXPECT_TRUE(channel1.get_impl()->has_rdma_send_stream());

    // Group commit flow over the established session. Verified here rather than in a
    // separate test because every establishment costs vendor-mock endpoint slots
    // (pid-keyed, max 64 per shm epoch, never freed) via the two daemon processes.
    // Swap the real control channel send stream for a capturing fake so that the
    // serialized bytes can be inspected without a replica-side receiver.
    auto stream = std::make_unique<capturing_control_send_stream>();
    auto* captured = stream.get();
    impl->set_rdma_control_send_stream_for_test(std::move(stream));

    EXPECT_TRUE(impl->propagate_group_commit(42U));
    ASSERT_EQ(captured->submitted_.size(), 1U);
    std::string const payload(captured->submitted_[0].begin(), captured->submitted_[0].end());
    limestone::replication::replication_message_io io{payload};
    auto message = limestone::replication::replication_message::receive(io);
    ASSERT_EQ(message->get_message_type_id(),
        limestone::replication::message_type_id::GROUP_COMMIT);
    auto* group_commit =
        dynamic_cast<limestone::replication::message_group_commit*>(message.get());
    ASSERT_NE(group_commit, nullptr);
    EXPECT_EQ(group_commit->epoch_number(), 42U);

    // The ACK wait maps onto flush(): not called by propagate, called once by wait.
    EXPECT_EQ(captured->flush_count_, 0U);
    impl->wait_for_propagated_group_commit_ack();
    EXPECT_EQ(captured->flush_count_, 1U);

    ds->shutdown().wait();
}

TEST_F(rdma_establish_session_test, establish_fails_fast_without_acceptor) {
    ASSERT_NO_FATAL_FAILURE(start_daemons());

    datastore_impl impl{};
    // No acceptor is seated: the daemon rejects the start (out_of_order) and there is
    // no retry, so the call fails immediately and leaves no RDMA state behind.
    EXPECT_FALSE(impl.establish_rdma_session());
    EXPECT_EQ(impl.get_rdma_sender(), nullptr);
    EXPECT_EQ(impl.get_rdma_control_send_stream(), nullptr);
    EXPECT_FALSE(impl.has_replica());
}

TEST_F(rdma_establish_session_test, group_commit_not_sent_before_session_established) {
    datastore_impl impl{};
    EXPECT_TRUE(impl.is_replication_configured());
    auto stream = std::make_unique<capturing_control_send_stream>();
    auto* captured = stream.get();
    impl.set_rdma_control_send_stream_for_test(std::move(stream));
    // No replica exists until the RDMA session is established, so nothing is sent.
    EXPECT_FALSE(impl.propagate_group_commit(1U));
    EXPECT_TRUE(captured->submitted_.empty());
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
