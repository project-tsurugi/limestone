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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <datastore_impl.h>
#include <log_channel_impl.h>
#include <rdma/handshake_client_base.h>
#include <rdma/rdma_factory.h>
#include <rdma/rdma_frame_buffer_base.h>
#include <rdma/rdma_handshake_payload.h>
#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_send_stream_base.h>
#include <rdma/rdma_sender_base.h>
#include <replication/replication_message.h>

#include "mock_handshake_client.h"
#include "test_rdma_frame_buffer.h"

namespace limestone::testing {

namespace {

using limestone::api::datastore_impl;
using limestone::replication::decode_start_payload;
using limestone::replication::encode;
using limestone::replication::handshake_connector_create_result;
using limestone::replication::rdma_handshake_operation_timeout;
using limestone::replication::rdma_handshake_response_payload;
using limestone::replication::rdma_receive_handler;
using limestone::replication::rdma_receiver_base;
using limestone::replication::rdma_send_stream_base;
using limestone::replication::rdma_sender_base;
using limestone::replication::replication_protocol_version;

constexpr std::uint64_t service_id = 57U;
constexpr std::uint32_t slot_count = 4U;

// Arbitrary non-zero sentinels; nothing in the fake stack dereferences them.
constexpr std::uint64_t fake_master_dma_address = 0xACC0FFEEULL;
constexpr std::uint64_t fake_replica_dma_address = 0xBEEFULL;

/**
 * @brief Scripted results and call record shared between a test and the fake
 *        RDMA stack (ACK receiver, data sender, send streams).
 *
 * The fake instances are owned by datastore_impl and destroyed during the
 * failure rollback, so the observable state lives here, owned by the test,
 * and survives them.
 */
struct fake_rdma_stack_state {
    // Scripted results; the defaults describe a fully working stack.
    rdma_receiver_base::operation_result receiver_initialize_result{true, {}};
    bool dma_address_available{true};
    rdma_receiver_base::operation_result receiver_bind_result{true, {}};
    rdma_sender_base::operation_result sender_initialize_result{true, {}};
    rdma_sender_base::operation_result stream_acquire_status{true, {}};
    // Number of leading get_send_stream() calls that succeed even when
    // stream_acquire_status scripts a failure. Data-channel acquisition failures
    // are fatal by design, so a scripted failure must be aimed past them at the
    // control channel's acquisition.
    std::size_t stream_acquire_ok_calls{0};
    rdma_sender_base::operation_result sender_finalize_result{true, {}};

    // Factory call record.
    int receivers_created{};
    int senders_created{};
    std::uint32_t receiver_slot_count{};
    std::uint32_t sender_slot_count{};

    // Destruction record; created minus destroyed is the number of instances
    // datastore_impl still holds.
    int receivers_destroyed{};
    int senders_destroyed{};

    // Instance call record.
    std::uint64_t sender_remote_dma_address{};
    std::vector<std::uint16_t> acquired_stream_ids{};
    int receiver_bind_calls{};
    int sender_finalize_calls{};
    int receiver_shutdown_calls{};
    int sender_shutdown_calls{};
};

/**
 * @brief Send stream stand-in; every operation succeeds.
 */
class fake_send_stream : public rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<limestone::replication::rdma_frame_buffer_base>
    acquire_frame_buffer(std::size_t max_payload, std::size_t min_capacity) noexcept override {
        auto const capacity = std::max(granted_frame_capacity(max_payload), min_capacity);
        return std::make_unique<test_rdma_frame_buffer>(capacity);
    }

    [[nodiscard]] send_result submit_frame_buffer(
            limestone::replication::rdma_frame_buffer_base& /*frame*/,
            std::size_t payload_size) override {
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds /*timeout*/) noexcept override {
        return {true, ""};
    }
};

/**
 * @brief ACK receiver stand-in returning the shared state's scripted results.
 */
class fake_ack_receiver : public rdma_receiver_base {
public:
    explicit fake_ack_receiver(fake_rdma_stack_state& state) noexcept : state_(state) {}

    ~fake_ack_receiver() override { ++state_.receivers_destroyed; }

    [[nodiscard]] operation_result initialize(rdma_receive_handler /*handler*/) noexcept override {
        return state_.receiver_initialize_result;
    }

    [[nodiscard]] operation_result shutdown() noexcept override {
        ++state_.receiver_shutdown_calls;
        return {true, {}};
    }

    [[nodiscard]] std::optional<std::uint64_t> get_dma_address() const noexcept override {
        if (!state_.dma_address_available) {
            return std::nullopt;
        }
        return fake_master_dma_address;
    }

    [[nodiscard]] operation_result finalize_channel_setup_with_sender(
            rdma_sender_base* /*sender*/) noexcept override {
        ++state_.receiver_bind_calls;
        return state_.receiver_bind_result;
    }

private:
    fake_rdma_stack_state& state_;
};

/**
 * @brief Data sender stand-in returning the shared state's scripted results.
 */
class fake_data_sender : public rdma_sender_base {
public:
    explicit fake_data_sender(fake_rdma_stack_state& state) noexcept : state_(state) {}

    ~fake_data_sender() override { ++state_.senders_destroyed; }

    [[nodiscard]] operation_result initialize(std::uint64_t remote_dma_address) noexcept override {
        state_.sender_remote_dma_address = remote_dma_address;
        return state_.sender_initialize_result;
    }

    [[nodiscard]] stream_acquire_result get_send_stream(std::uint16_t channel_id) noexcept override {
        state_.acquired_stream_ids.push_back(channel_id);
        if (!state_.stream_acquire_status.success
            && state_.acquired_stream_ids.size() > state_.stream_acquire_ok_calls) {
            return {state_.stream_acquire_status, nullptr};
        }
        return {{true, {}}, std::make_unique<fake_send_stream>()};
    }

    [[nodiscard]] operation_result finalize_channel_setup() noexcept override {
        ++state_.sender_finalize_calls;
        return state_.sender_finalize_result;
    }

    [[nodiscard]] operation_result shutdown() noexcept override {
        ++state_.sender_shutdown_calls;
        return {true, {}};
    }

private:
    fake_rdma_stack_state& state_;
};

/**
 * @brief Gives the tests access to log_channel's protected constructor.
 */
class test_log_channel : public limestone::api::log_channel {
public:
    test_log_channel(boost::filesystem::path location, std::size_t id,
                     limestone::api::datastore& envelope) noexcept
        : log_channel(std::move(location), id, envelope) {}
};

} // namespace

/**
 * @brief Master-side establish_rdma_session coverage with the handshake connector
 *        and the RDMA stack replaced by scripted fakes.
 *
 * Complements rdma_establish_session_test: no handshake daemon and no vendor
 * RDMA mock are involved, so every handshake step and every stack operation can
 * be failed deterministically, and the rollback behavior of each failure path
 * can be asserted. The daemon-backed test keeps covering the real wiring.
 */
class rdma_establish_session_mock_test : public ::testing::Test {
protected:
    static constexpr char const* handshake_socket_path = "mock-handshake.sock";

    void SetUp() override {
        // The datastore_impl constructor loads the replication configuration from the
        // environment. The socket path is never opened; the connector is mocked.
        ::setenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", handshake_socket_path, 1);
        ::setenv("TSURUGI_REPLICATION_SERVICE_ID", "57", 1);
        ::setenv("REPLICATION_RDMA_SLOTS", "4", 1);
        ::unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    }

    void TearDown() override {
        ::unsetenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET");
        ::unsetenv("TSURUGI_REPLICATION_SERVICE_ID");
        ::unsetenv("REPLICATION_RDMA_SLOTS");
    }

    // Installs a factory that records its arguments into script_ and hands out a
    // connector driven by script_.
    void install_mock_connector(datastore_impl& impl) {
        impl.set_handshake_connector_factory_for_test(
            [this](std::string const& path, std::chrono::milliseconds timeout) {
                ++script_.factory_calls;
                script_.daemon_socket_path = path;
                script_.operation_timeout = timeout;
                return handshake_connector_create_result{
                    {true, {}}, std::make_unique<mock_handshake_connector>(script_)};
            });
    }

    // Installs a factory whose creation itself fails.
    void install_failing_connector_factory(datastore_impl& impl) {
        impl.set_handshake_connector_factory_for_test(
            [this](std::string const& /*path*/, std::chrono::milliseconds /*timeout*/) {
                ++script_.factory_calls;
                return handshake_connector_create_result{
                    {false, "scripted connector creation failure"}, nullptr};
            });
    }

    void install_fake_rdma_stack(datastore_impl& impl) {
        impl.set_rdma_ack_receiver_factory_for_test(
            [this](std::uint32_t slots) -> std::unique_ptr<rdma_receiver_base> {
                ++state_.receivers_created;
                state_.receiver_slot_count = slots;
                return std::make_unique<fake_ack_receiver>(state_);
            });
        impl.set_rdma_data_sender_factory_for_test(
            [this](std::uint32_t slots) -> std::unique_ptr<rdma_sender_base> {
                ++state_.senders_created;
                state_.sender_slot_count = slots;
                return std::make_unique<fake_data_sender>(state_);
            });
    }

    void install_all(datastore_impl& impl) {
        install_mock_connector(impl);
        install_fake_rdma_stack(impl);
    }

    // establish_rdma_session() rejects a channel count of zero, so give the impl
    // under test one log channel. The channel never begins a session here, so it
    // performs no file I/O.
    void register_one_channel(datastore_impl& impl) {
        impl.register_log_channel([this](std::uint64_t id) {
            return std::unique_ptr<limestone::api::log_channel>(
                new test_log_channel("/tmp/rdma_establish_session_mock_test", id, envelope_));
        });
    }

    [[nodiscard]] static std::vector<std::uint8_t> accepted_response() {
        rdma_handshake_response_payload response{};
        response.accepted = true;
        response.replica_dma_address = fake_replica_dma_address;
        return encode(response);
    }

    // Asserts the state every failure path must leave behind: no sender, no control
    // stream, no replica, and no fake instance still held (a held instance would make
    // the next establish_rdma_session() fail on "ack_receiver already initialized").
    void expect_rolled_back(datastore_impl& impl) {
        EXPECT_EQ(impl.get_rdma_sender(), nullptr);
        EXPECT_EQ(impl.get_rdma_control_send_stream(), nullptr);
        EXPECT_FALSE(impl.has_replica());
        EXPECT_EQ(state_.receivers_destroyed, state_.receivers_created);
        EXPECT_EQ(state_.senders_destroyed, state_.senders_created);
    }

    mock_handshake_connector_script script_{};
    fake_rdma_stack_state state_{};
    limestone::api::datastore envelope_{};
};

TEST_F(rdma_establish_session_mock_test, establish_fails_when_replication_mode_is_not_rdma) {
    ::unsetenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET");
    ::unsetenv("TSURUGI_REPLICATION_SERVICE_ID");
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.receivers_created, 0);
    EXPECT_EQ(script_.factory_calls, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_slot_count_is_not_configured) {
    // The RDMA mode is decided by the handshake settings alone, so dropping only the
    // slot count reaches the defensive guard behind the mode check.
    ::unsetenv("REPLICATION_RDMA_SLOTS");
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.receivers_created, 0);
    EXPECT_EQ(script_.factory_calls, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_ack_receiver_initialize_fails) {
    state_.receiver_initialize_result = {false, "scripted receiver initialize failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    // The receiver is discarded without a shutdown (it never came up), and the
    // handshake is never started.
    EXPECT_EQ(script_.factory_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 0);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_ack_receiver_dma_address_is_unavailable) {
    state_.dma_address_available = false;
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.factory_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_connector_creation_fails) {
    datastore_impl impl{};
    register_one_channel(impl);
    install_failing_connector_factory(impl);
    install_fake_rdma_stack(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.factory_calls, 1);
    EXPECT_EQ(script_.start_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_handshake_start_fails) {
    script_.start_result = {false, "scripted start failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.start_calls, 1);
    EXPECT_EQ(script_.receive_response_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_response_receive_fails) {
    script_.response_result = {false, "scripted response failure", {}};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.receive_response_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_on_malformed_response_payload) {
    script_.response_result = {true, {}, {0x01U, 0x02U, 0x03U}};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.receive_response_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_replica_rejects_session) {
    rdma_handshake_response_payload rejection{};
    rejection.accepted = false;
    rejection.error_message = "scripted rejection";
    script_.response_result = {true, {}, encode(rejection)};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.receive_response_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(state_.senders_created, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_sender_initialize_fails) {
    script_.response_result = {true, {}, accepted_response()};
    state_.sender_initialize_result = {false, "scripted sender initialize failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.senders_created, 1);
    EXPECT_EQ(state_.sender_remote_dma_address, fake_replica_dma_address);
    // The sender is discarded without a shutdown (it never came up).
    EXPECT_EQ(state_.sender_shutdown_calls, 0);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_control_stream_acquisition_fails) {
    script_.response_result = {true, {}, accepted_response()};
    state_.stream_acquire_status = {false, "scripted stream acquisition failure"};
    state_.stream_acquire_ok_calls = 1;  // data channel 0 succeeds; the control channel fails
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    // Data channel 0's stream is acquired first; the control channel's (id 1)
    // acquisition fails.
    EXPECT_EQ(state_.acquired_stream_ids, (std::vector<std::uint16_t>{0U, 1U}));
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_ack_receiver_bind_fails) {
    script_.response_result = {true, {}, accepted_response()};
    state_.receiver_bind_result = {false, "scripted bind failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.receiver_bind_calls, 1);
    EXPECT_EQ(state_.sender_finalize_calls, 0);
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_sender_finalize_fails) {
    script_.response_result = {true, {}, accepted_response()};
    state_.sender_finalize_result = {false, "scripted sender finalize failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.sender_finalize_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 0);
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_handshake_finalize_fails) {
    script_.response_result = {true, {}, accepted_response()};
    script_.finalize_result = {false, "scripted finalize failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(state_.sender_finalize_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 1);
    EXPECT_EQ(script_.send_ready_calls, 0);
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_handshake_ready_fails) {
    script_.response_result = {true, {}, accepted_response()};
    script_.ready_result = {false, "scripted ready failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.send_ready_calls, 1);
    EXPECT_EQ(script_.receive_completion_calls, 0);
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_fails_when_handshake_completion_fails) {
    script_.response_result = {true, {}, accepted_response()};
    script_.completion_result = {false, "scripted completion failure"};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_FALSE(impl.establish_rdma_session());

    EXPECT_EQ(script_.receive_completion_calls, 1);
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
    expect_rolled_back(impl);
}

TEST_F(rdma_establish_session_mock_test, establish_succeeds_with_mocked_handshake_and_stack) {
    script_.response_result = {true, {}, accepted_response()};
    datastore_impl impl{};
    register_one_channel(impl);
    install_all(impl);

    EXPECT_TRUE(impl.establish_rdma_session());

    // Factory arguments come from the replication configuration.
    EXPECT_EQ(script_.factory_calls, 1);
    EXPECT_EQ(script_.daemon_socket_path, handshake_socket_path);
    EXPECT_EQ(script_.operation_timeout, rdma_handshake_operation_timeout);

    // One call per handshake step, in protocol order.
    EXPECT_EQ(script_.start_calls, 1);
    EXPECT_EQ(script_.receive_response_calls, 1);
    EXPECT_EQ(script_.send_finalize_calls, 1);
    EXPECT_EQ(script_.send_ready_calls, 1);
    EXPECT_EQ(script_.receive_completion_calls, 1);
    EXPECT_EQ(script_.target_service, service_id);
    EXPECT_TRUE(script_.finalize_payload.empty());

    auto const start = decode_start_payload(script_.start_payload);
    ASSERT_TRUE(start.has_value()) << "the master sent a malformed start payload";
    EXPECT_EQ(start->protocol_version, replication_protocol_version);
    EXPECT_TRUE(start->configuration_id.empty());
    EXPECT_EQ(start->epoch_number, 0U);
    EXPECT_EQ(start->slot_count, slot_count);
    EXPECT_EQ(start->master_dma_address, fake_master_dma_address);
    EXPECT_EQ(start->channel_count, 1U);
    EXPECT_EQ(start->control_channel_id, 1U);

    // The fake stack got the configured slot count and the replica's DMA address,
    // and was driven through bind and finalize exactly once.
    EXPECT_EQ(state_.receivers_created, 1);
    EXPECT_EQ(state_.senders_created, 1);
    EXPECT_EQ(state_.receiver_slot_count, slot_count);
    EXPECT_EQ(state_.sender_slot_count, slot_count);
    EXPECT_EQ(state_.sender_remote_dma_address, fake_replica_dma_address);
    // Data channel 0 first, then the control channel (id 1).
    EXPECT_EQ(state_.acquired_stream_ids, (std::vector<std::uint16_t>{0U, 1U}));
    EXPECT_EQ(state_.receiver_bind_calls, 1);
    EXPECT_EQ(state_.sender_finalize_calls, 1);

    EXPECT_NE(impl.get_rdma_control_send_stream(), nullptr);
    EXPECT_TRUE(impl.has_replica());

    EXPECT_TRUE(impl.shutdown_rdma_sender());
    EXPECT_TRUE(impl.shutdown_rdma_ack_receiver());
    EXPECT_EQ(state_.sender_shutdown_calls, 1);
    EXPECT_EQ(state_.receiver_shutdown_calls, 1);
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
