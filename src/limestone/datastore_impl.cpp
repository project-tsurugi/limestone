/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include <datastore_impl.h>
#include <limestone/logging.h>
#include <logging_helper.h>
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <stdexcept>
#include <string>
#include <cctype>
#include <string_view>
#include <limits>
#include <cerrno>
#include <functional>
#include <mutex>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#ifdef ENABLE_ALTIMETER
#include <altimeter/event/constants.h>
#include <altimeter/log_item.h>
#include <altimeter/logger.h>
#endif

#include <replication/replica_connector.h>
#include <limestone_exception_helper.h>
#include <replication/message_session_begin.h>
#include <replication/message_log_channel_create.h>
#include <replication/message_group_commit.h>
#include <replication/message_error.h>
#include <replication/replication_message.h>
#include <replication/replication_message_io.h>
#include <rdma/rdma_factory.h>
#include <rdma/rdma_handshake_payload.h>
#include <rdma/rdma_receive_event.h>
#include <log_channel_impl.h>
#include <manifest.h>

namespace limestone::api {

// Default constructor initializes the backup counter to zero.
datastore_impl::datastore_impl()
    : backup_counter_(0)
    , replica_exists_(false)
    , async_session_close_enabled_(std::getenv("REPLICATION_ASYNC_SESSION_CLOSE") != nullptr)
    , async_group_commit_enabled_(std::getenv("REPLICATION_ASYNC_GROUP_COMMIT") != nullptr)
    , rdma_slot_count_(std::nullopt)
    , migration_info_(std::nullopt)
{
    LOG_LP(INFO) << "REPLICATION_ASYNC_SESSION_CLOSE: "
                 << (async_session_close_enabled_ ? "enabled" : "disabled");
    LOG_LP(INFO) << "REPLICATION_ASYNC_GROUP_COMMIT: "
                 << (async_group_commit_enabled_ ? "enabled" : "disabled");
    initialize_rdma_slots();

    bool has_replica = replication_endpoint_.is_valid();
    replica_exists_.store(has_replica, std::memory_order_release);
    LOG_LP(INFO) << "Replica " << (has_replica ? "enabled" : "disabled")
                    << "; endpoint valid: " << replication_endpoint_.is_valid();
    LOG_LP(INFO) << "Replication mode: " << replication_config_result_.config.mode();
    
    // Generate HMAC secret key for BLOB reference tag generation
    generate_hmac_secret_key();
}

// Default destructor.
datastore_impl::~datastore_impl() = default;

// Increments the backup counter.
void datastore_impl::increment_backup_counter() noexcept {
    backup_counter_.fetch_add(1, std::memory_order_acq_rel);
    LOG_LP(INFO) << "Beginning backup; active backup count: " << backup_counter_.load(std::memory_order_acquire);
}

// Decrements the backup counter.
void datastore_impl::decrement_backup_counter() noexcept {
    backup_counter_.fetch_sub(1, std::memory_order_acq_rel);
    LOG_LP(INFO) << "Ending backup; active backup count: " << backup_counter_.load(std::memory_order_acquire);
}

// Returns true if a backup operation is in progress.
bool datastore_impl::is_backup_in_progress() const noexcept {
    int count = backup_counter_.load(std::memory_order_acquire);
    VLOG_LP(log_trace) << "Checking if backup is in progress; active backup count: " << count;
    return count > 0;
}

// Returns true if a replica exists.
bool datastore_impl::has_replica() const noexcept {
    bool exists = replica_exists_.load(std::memory_order_acquire);
    VLOG_LP(log_trace) << "Checking replica existence; replica exists: " << exists;
    return exists;
}

// Disables the replica.
void datastore_impl::disable_replica() noexcept {
    replica_exists_.store(false, std::memory_order_release);
    LOG_LP(INFO) << "Replica disabled";
}

bool datastore_impl::connect_control_channel() {
    if (!replication_endpoint_.is_valid()) {
        LOG_LP(ERROR) << "Invalid replication endpoint.";
        return false;
    }

    std::string host = replication_endpoint_.host();
    int port = replication_endpoint_.port();

    control_channel_ = std::make_shared<replica_connector>();
    if (!control_channel_->connect_to_server(host, port)) {
        LOG_LP(ERROR) << "Failed to connect to control channel at " << host << ":" << port;
        return false;
    }
    return true;
}

bool datastore_impl::send_session_begin() {
    auto request = message_session_begin::create();
    if (!control_channel_->send_message(*request)) {
        LOG_LP(ERROR) << "Failed to send session begin message.";
        return false;
    }

    auto response = control_channel_->receive_message();
    if (response == nullptr) {
        LOG_LP(ERROR) << "Failed to receive session begin acknowledgment.";
        return false;
    }

    if (response->get_message_type_id() == message_type_id::COMMON_ERROR) {
        auto* err = dynamic_cast<message_error*>(response.get());
        std::string msg = err ? err->get_error_message()
                              : "Session begin failed with unknown error response";
        LOG_LP(FATAL) << msg;
    }

    if (response->get_message_type_id() != message_type_id::SESSION_BEGIN_ACK) {
        LOG_LP(ERROR) << "Failed to receive session begin acknowledgment.";
        return false;
    }
    return true;
}

// Method to open the control channel
bool datastore_impl::open_control_channel() {
    TRACE_START;
    if (!connect_control_channel()) {
        replica_exists_.store(false, std::memory_order_release);
        TRACE_END;
        return false;
    }

    if (!send_session_begin()) {
        replica_exists_.store(false, std::memory_order_release);
        control_channel_->close_session();
        TRACE_END;
        return false;
    }

    LOG_LP(INFO) << "Control channel successfully opened to " << replication_endpoint_.host()
                 << ":" << replication_endpoint_.port();
    TRACE_END;
    return true;
}

bool datastore_impl::propagate_group_commit(uint64_t epoch_id) {
    if (!is_master_) {
        return false;
    }
    if (replica_exists_.load(std::memory_order_acquire)) {
        TRACE_START << "epoch_id=" << epoch_id;
        bool sent = false;
        if (group_commit_sender_for_tests_) {
            sent = group_commit_sender_for_tests_(epoch_id);
        } else if (replication_config_result_.config.mode() == replication_mode::rdma) {
            sent = propagate_group_commit_rdma(epoch_id);
        } else {
            if (!control_channel_) {
                LOG_LP(ERROR) << "Control channel is not initialized.";
                TRACE_END << "Failed to send group commit message.";
                sent = false;
            } else {
                message_group_commit message{epoch_id};
                sent = control_channel_->send_message(message);
            }
        }
        if (!sent) {
            LOG_LP(ERROR) << "Failed to send group commit message to replica.";
            TRACE_END << "Failed to send group commit message.";
#ifdef ENABLE_ALTIMETER
            if (::altimeter::logger::is_log_on(::altimeter::event::category,
                                               ::altimeter::event::level::log_data_store)) {
                ::altimeter::log_item log_item;
                log_item.category(::altimeter::event::category);
                log_item.type(::altimeter::event::type::wal_shipped);
                log_item.level(::altimeter::event::level::log_data_store);
                log_item.add(::altimeter::event::item::instance_id, instance_id_);
                log_item.add(::altimeter::event::item::dbname, db_name_);
                log_item.add(::altimeter::event::item::pid, static_cast<std::int64_t>(pid_));
                std::string wal_version = std::to_string(epoch_id);
                log_item.add(::altimeter::event::item::wal_version, wal_version);
                log_item.add(::altimeter::event::item::result, ::altimeter::event::result::failure);
                ::altimeter::logger::log(log_item);
            }
#endif
            return false;
        }
        TRACE_END;
#ifdef ENABLE_ALTIMETER
        if (::altimeter::logger::is_log_on(::altimeter::event::category,
                                           ::altimeter::event::level::log_data_store)) {
            ::altimeter::log_item log_item;
            log_item.category(::altimeter::event::category);
            log_item.type(::altimeter::event::type::wal_shipped);
            log_item.level(::altimeter::event::level::log_data_store);
            log_item.add(::altimeter::event::item::instance_id, instance_id_);
            log_item.add(::altimeter::event::item::dbname, db_name_);
            log_item.add(::altimeter::event::item::pid, static_cast<std::int64_t>(pid_));
            std::string wal_version = std::to_string(epoch_id);
            log_item.add(::altimeter::event::item::wal_version, wal_version);
            log_item.add(::altimeter::event::item::result, ::altimeter::event::result::success);
            ::altimeter::logger::log(log_item);
        }
#endif
        return true;
    }
    return false;
}

bool datastore_impl::propagate_group_commit_rdma(uint64_t epoch_id) {
    if (!rdma_control_send_stream_) {
        LOG_LP(ERROR) << "RDMA control channel send stream is not initialized.";
        return false;
    }
    replication_message_io io{std::string{}};
    message_group_commit message{epoch_id};
    replication_message::send(io, message);
    // rdma_send_stream is not thread-safe and no mutex is taken here: the caller is
    // already serialized by the CAS in update_min_epoch_id() and runs inside the
    // lock_guard of mtx_epoch_file_ (plus one pre-threading call in ready()). If that
    // premise ever breaks, multiple threads would touch this stream concurrently.
    auto result = rdma_control_send_stream_->send_all_bytes(io.get_out_string());
    if (!result.success) {
        // acquire_frame_buffer() absorbs backpressure by blocking, so a failure means
        // the send ring never drained within the transport timeout, i.e. the replica
        // is gone. Unlike the TCP path, dropping the replica and continuing is not an
        // option: the send buffer ring is shared by every channel, so undrained control
        // frames eventually stall the WAL data channels as well.
        LOG_LP(FATAL) << "Failed to send the group commit message over the RDMA control "
            << "channel: " << result.error_message;
        return false;
    }
    return true;
}

void datastore_impl::wait_for_propagated_group_commit_ack() {
    TRACE_START;
    if (group_commit_sender_for_tests_) {
        // The test hook sends synchronously in propagate_group_commit(), so there is
        // nothing to wait for; mirrors the hook-first branching on the send side.
        TRACE_END;
        return;
    }
    if (replication_config_result_.config.mode() == replication_mode::rdma) {
        if (!rdma_control_send_stream_) {
            LOG_LP(ERROR) << "RDMA control channel send stream is not initialized.";
            TRACE_END << "No RDMA control channel send stream.";
            return;
        }
        // The replica-side receive handler runs synchronously before the transport
        // sends the ACK frame back, so the completion of flush() means the replica
        // has finished persist_and_propagate_epoch_id() for every submitted frame.
        auto flush_result = rdma_control_send_stream_->flush(rdma_flush_timeout);
        if (!flush_result.success) {
            // Fatal for the same shared-ring reason as in propagate_group_commit_rdma().
            LOG_LP(FATAL) << "RDMA control channel flush failed: "
                << flush_result.error_message;
        }
        TRACE_END;
        return;
    }
    auto response = control_channel_->receive_message();
    if (response == nullptr || response->get_message_type_id() != message_type_id::COMMON_ACK) {
        LOG_LP(ERROR) << "Failed to receive acknowledgment for switch epoch message.";
        control_channel_->close_session();
        replica_exists_.store(false, std::memory_order_release);
        TRACE_END << "Failed to receive acknowledgment for switch epoch message.";
        return;
    }
    TRACE_END;
}

bool datastore_impl::is_replication_configured() const noexcept {
    return replication_config_result_.config.mode() != replication_mode::none;
}

replication::replication_config_parse_result const&
datastore_impl::get_replication_config_result() const noexcept {
    return replication_config_result_;
}

// Getter for control_channel_
std::shared_ptr<replica_connector> datastore_impl::get_control_channel() const noexcept {
    return control_channel_;
}

std::unique_ptr<replication::replica_connector> datastore_impl::create_log_channel_connector(datastore& ds, std::uint64_t channel_id) {
    TRACE_START;
    if (!replica_exists_.load(std::memory_order_acquire)) {
        TRACE_END << "No replica exists, cannot create log channel connector.";
        return nullptr;
    }
    if (log_channel_connector_factory_for_test_) {
        return log_channel_connector_factory_for_test_();
    }
    auto connector = std::make_unique<replica_connector>();

    std::string host = replication_endpoint_.host();  
    int port = replication_endpoint_.port();          
    if (!connector->connect_to_server(host, port, ds)) {  
        LOG_LP(ERROR) << "Failed to connect to control channel at " << host << ":" << port;
        replica_exists_.store(false, std::memory_order_release);
        return nullptr;
    }

    auto request = std::make_unique<message_log_channel_create>(channel_id);
    if (!connector->send_message(*request)) {
        LOG_LP(ERROR) << "Failed to send log channel create message.";
        replica_exists_.store(false, std::memory_order_release);
        connector->close_session();
        return nullptr;
    }

    auto response = connector->receive_message();
    if (response == nullptr || response->get_message_type_id() != message_type_id::COMMON_ACK) {
        LOG_LP(ERROR) << "Failed to receive acknowledgment.";
        replica_exists_.store(false, std::memory_order_release);
        connector->close_session();
        return nullptr;
    }

    LOG_LP(INFO) << "Log channel successfully created to " << host << ":" << port;
    TRACE_END;
    return connector;
}

log_channel& datastore_impl::register_log_channel(
    std::function<std::unique_ptr<log_channel>(std::uint64_t)> const& factory) {
    std::lock_guard<std::mutex> lock(mtx_channel_);
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(factory(id));
    return *log_channels_.back();
}

std::vector<std::unique_ptr<log_channel>> const& datastore_impl::log_channels() const noexcept {
    return log_channels_;
}

void datastore_impl::maybe_register_rdma_stream(log_channel& channel, std::size_t id) {
    auto acquire_stream = [&](auto&& acquire_fn) {
        auto stream_result = acquire_fn(static_cast<std::uint16_t>(id));
        if (! stream_result.status.success || stream_result.stream == nullptr) {
            LOG_LP(FATAL) << "Failed to acquire RDMA send stream: "
                          << stream_result.status.error_message;
        }
        channel.get_impl()->set_rdma_send_stream(std::move(stream_result.stream));
    };

    if (rdma_stream_factory_for_test_) {
        acquire_stream(rdma_stream_factory_for_test_);
        return;
    }

    if (rdma_sender_ == nullptr || ! is_rdma_enabled()) {
        return;
    }
    if (id > std::numeric_limits<std::uint16_t>::max()) {
        LOG_LP(FATAL) << "RDMA channel_id overflow: id=" << id;
    }
    acquire_stream([this](std::uint16_t cid) {
        return rdma_sender_->get_send_stream(cid);
    });
}

void datastore_impl::set_replica_role() noexcept {
    is_master_ = false;
}

bool datastore_impl::is_master() const noexcept {
    return is_master_;
}

bool datastore_impl::is_async_session_close_enabled() const noexcept {
    return async_session_close_enabled_;
}

bool datastore_impl::is_async_group_commit_enabled() const noexcept {
    return async_group_commit_enabled_;
}

void datastore_impl::set_group_commit_sender_for_tests(std::function<bool(uint64_t)> const& sender) {
    group_commit_sender_for_tests_ = sender;
}

void datastore_impl::set_instance_id(std::string_view instance_id) {
    instance_id_ = instance_id;
}

const std::string& datastore_impl::instance_id() const noexcept {
    return instance_id_;
}

void datastore_impl::set_db_name(std::string_view db_name) {
    db_name_ = db_name;
}

const std::string& datastore_impl::db_name() const noexcept {
    return db_name_;
}

void datastore_impl::set_pid(pid_t pid) noexcept {
    pid_ = pid;
}

pid_t datastore_impl::pid() const noexcept {
    return pid_;
}

bool datastore_impl::is_rdma_enabled() const noexcept {
    return rdma_slot_count_.has_value();
}

std::optional<std::int32_t> datastore_impl::rdma_slot_count() const noexcept {
    return rdma_slot_count_;
}

void datastore_impl::initialize_rdma_slots() {
    const char* env_val = std::getenv("REPLICATION_RDMA_SLOTS");
    if (env_val == nullptr) {
        LOG_LP(INFO) << "REPLICATION_RDMA_SLOTS: not set; RDMA replication disabled";
        return;
    }

    bool all_whitespace = true;
    std::string_view env_view{env_val};
    for (char ch : env_view) {
        if (std::isspace(static_cast<unsigned char>(ch)) == 0) {
            all_whitespace = false;
            break;
        }
    }
    if (all_whitespace) {
        LOG_LP(ERROR) << "Invalid REPLICATION_RDMA_SLOTS: whitespace only; RDMA replication disabled";
        rdma_slot_count_ = std::nullopt;
        return;
    }

    char* endptr = nullptr;
    errno = 0;
    std::int64_t parsed = std::strtoll(env_val, &endptr, 10);
    // Check for range errors reported by strtoll
    if (errno == ERANGE) {
        LOG_LP(ERROR) << "Invalid REPLICATION_RDMA_SLOTS: out of range; "
                      << "RDMA replication disabled";
        rdma_slot_count_ = std::nullopt;
        return;
    }
    // Check if no conversion was performed
    if (endptr == env_val) {
        LOG_LP(ERROR) << "Invalid REPLICATION_RDMA_SLOTS: non-numeric value; "
                      << "RDMA replication disabled";
        rdma_slot_count_ = std::nullopt;
        return;
    }
    // Check for extra characters after the number
    if (*endptr != '\0') {
        LOG_LP(ERROR) << "Invalid REPLICATION_RDMA_SLOTS: trailing characters; "
                      << "RDMA replication disabled";
        rdma_slot_count_ = std::nullopt;
        return;
    }
    // ERANGE only covers values outside strtoll's range; the RDMA slot count
    // must also fit in the 32-bit protocol/configuration field.
    if (parsed <= 0 || parsed > std::numeric_limits<std::int32_t>::max()) {
        LOG_LP(ERROR) << "Invalid REPLICATION_RDMA_SLOTS: value must be 1..INT32_MAX; "
                      << "RDMA replication disabled";
        rdma_slot_count_ = std::nullopt;
        return;
    }

    rdma_slot_count_ = static_cast<std::int32_t>(parsed);
    LOG_LP(INFO) << "REPLICATION_RDMA_SLOTS: enabled with " << rdma_slot_count_.value()
                 << " slots (4KB each)";
}

bool datastore_impl::establish_rdma_session() {
    auto const& config = replication_config_result_.config;
    if (config.mode() != replication_mode::rdma) {
        LOG_LP(ERROR) << "RDMA session establishment requested but the replication mode is "
            << config.mode() << ".";
        return false;
    }
    // This branch should be unreachable because RDMA mode requires REPLICATION_RDMA_SLOTS
    // at startup, but kept for defense.
    if (!rdma_slot_count_.has_value() || rdma_slot_count_.value() <= 0) {
        LOG_LP(ERROR) << "RDMA slot count is not configured.";
        return false;
    }
    auto slot_count = static_cast<std::uint32_t>(rdma_slot_count_.value());

    if (log_channels_.size() > std::numeric_limits<std::uint16_t>::max()) {
        LOG_LP(ERROR) << "Too many log channels for RDMA replication: " << log_channels_.size();
        return false;
    }
    auto channel_count = static_cast<std::uint16_t>(log_channels_.size());
    // Data channels take ids 0 .. channel_count - 1; the control channel takes the
    // next id. The value is carried in the payload, so the replica does not rely on
    // this assignment rule.
    auto control_channel_id = channel_count;

    auto master_dma_address = initialize_rdma_ack_receiver(slot_count);
    if (!master_dma_address.has_value()) {
        LOG_LP(ERROR) << "Failed to initialize the RDMA ACK receiver.";
        return false;
    }

    auto fail = [this]() {
        // Streams must be released before the sender that owns them is shut down.
        release_rdma_send_streams();
        (void) shutdown_rdma_sender();
        (void) shutdown_rdma_ack_receiver();
        return false;
    };

    // configuration_id and epoch_number follow the TCP SESSION_BEGIN behavior, which
    // sends the message default values (see send_session_begin()).
    rdma_handshake_start_payload start_payload{};
    start_payload.protocol_version = replication_protocol_version;
    start_payload.slot_count = slot_count;
    start_payload.master_dma_address = master_dma_address.value();
    start_payload.channel_count = channel_count;
    start_payload.control_channel_id = control_channel_id;

    auto connector_result = handshake_connector_factory_for_test_
        ? handshake_connector_factory_for_test_(
              config.handshake_socket_path(), rdma_handshake_operation_timeout)
        : make_handshake_connector(
              config.handshake_socket_path(), rdma_handshake_operation_timeout);
    if (!connector_result.status.success) {
        LOG_LP(ERROR) << "Failed to create the handshake connector: "
            << connector_result.status.error_message;
        return fail();
    }
    auto connector = std::move(connector_result.instance);

    auto start_result = connector->start(config.service_id(), encode(start_payload));
    if (!start_result.success) {
        LOG_LP(ERROR) << "Handshake start failed (is the replica already waiting on the "
            << "handshake daemon?): " << start_result.error_message;
        return fail();
    }

    auto response_result = connector->receive_response();
    if (!response_result.success) {
        LOG_LP(ERROR) << "Failed to receive the handshake response: "
            << response_result.error_message;
        return fail();
    }
    auto response = decode_response_payload(response_result.payload);
    if (!response.has_value()) {
        LOG_LP(ERROR) << "Received a malformed handshake response payload.";
        return fail();
    }
    if (!response->accepted) {
        LOG_LP(ERROR) << "Replica rejected the replication session: " << response->error_message;
        return fail();
    }

    if (!initialize_rdma_sender(slot_count, response->replica_dma_address)) {
        return fail();
    }

    for (std::size_t id = 0; id < log_channels_.size(); ++id) {
        auto* channel = log_channels_[id].get();
        if (channel == nullptr) {
            continue;
        }
        maybe_register_rdma_stream(*channel, id);
    }

    auto stream_result = rdma_sender_->get_send_stream(control_channel_id);
    if (!stream_result.status.success || stream_result.stream == nullptr) {
        LOG_LP(ERROR) << "Failed to acquire the control channel send stream: "
            << stream_result.status.error_message;
        return fail();
    }
    rdma_control_send_stream_ = std::move(stream_result.stream);

    // Bind the ack_receiver to the data sender so that ACK frames received from the replica
    // are routed to the data sender's send_streams (enabling flush() completion). Must happen
    // before the data sender transitions to TRANSFER phase.
    auto bind_result = ack_receiver_->finalize_channel_setup_with_sender(rdma_sender_.get());
    if (!bind_result.success) {
        LOG_LP(ERROR) << "ack_receiver::finalize_channel_setup_with_sender() failed: "
            << bind_result.error_message;
        return fail();
    }

    auto finalize_result = rdma_sender_->finalize_channel_setup();
    if (!finalize_result.success) {
        LOG_LP(ERROR) << "rdma_sender::finalize_channel_setup() failed: "
            << finalize_result.error_message;
        return fail();
    }

    auto send_finalize_result = connector->send_finalize({});
    if (!send_finalize_result.success) {
        LOG_LP(ERROR) << "Handshake finalize failed: " << send_finalize_result.error_message;
        return fail();
    }
    auto ready_result = connector->send_ready();
    if (!ready_result.success) {
        LOG_LP(ERROR) << "Handshake ready failed: " << ready_result.error_message;
        return fail();
    }
    auto completion_result = connector->receive_completion();
    if (!completion_result.success) {
        LOG_LP(ERROR) << "Handshake completion failed: " << completion_result.error_message;
        return fail();
    }

    replica_exists_.store(true, std::memory_order_release);
    LOG_LP(INFO) << "RDMA session established: " << start_payload
        << ", replica_dma_address=" << response->replica_dma_address;
    return true;
}

bool datastore_impl::establish_tcp_control_channel() {
    return open_control_channel();
}

rdma_send_stream_base* datastore_impl::get_rdma_control_send_stream() const noexcept {
    return rdma_control_send_stream_.get();
}

bool datastore_impl::initialize_rdma_sender(uint32_t slot_count, uint64_t remote_dma_address) {
    rdma_sender_ = rdma_data_sender_factory_for_test_
        ? rdma_data_sender_factory_for_test_(slot_count)
        : make_rdma_data_sender(slot_count);
    auto result = rdma_sender_->initialize(remote_dma_address);
    if (! result.success) {
        rdma_sender_.reset();
        LOG_LP(ERROR) << "rdma_sender::initialize() failed: " << result.error_message;
        return false;
    }
    return true;
}

rdma_sender_base* datastore_impl::get_rdma_sender() const noexcept {
    return rdma_sender_.get();
}

void datastore_impl::release_rdma_send_streams() noexcept {
    for (auto const& channel : log_channels_) {
        if (channel != nullptr) {
            channel->get_impl()->set_rdma_send_stream(nullptr);
        }
    }
}

bool datastore_impl::shutdown_rdma_sender() noexcept {
    rdma_control_send_stream_.reset();
    if (! rdma_sender_) {
        return true;
    }

    auto result = rdma_sender_->shutdown();
    if (! result.success) {
        LOG_LP(ERROR) << "rdma_sender::shutdown() failed: " << result.error_message;
        return false;
    }

    rdma_sender_.reset();
    return true;
}

std::optional<std::uint64_t> datastore_impl::initialize_rdma_ack_receiver(std::uint32_t slot_count) {
    if (ack_receiver_) {
        LOG_LP(ERROR) << "ack_receiver already initialized.";
        return std::nullopt;
    }

    ack_receiver_ = rdma_ack_receiver_factory_for_test_
        ? rdma_ack_receiver_factory_for_test_(slot_count)
        : make_rdma_ack_receiver(slot_count);
    auto result = ack_receiver_->initialize(
        // ACK frames are routed internally by the lib once finalize_channel_setup_with_sender
        // binds this receiver to the data sender; the user-supplied callback is unused.
        [](rdma_receive_event const&) {});
    if (! result.success) {
        LOG_LP(ERROR) << "ack_receiver::initialize() failed: " << result.error_message;
        ack_receiver_.reset();
        return std::nullopt;
    }

    auto dma_address = ack_receiver_->get_dma_address();
    if (! dma_address.has_value()) {
        LOG_LP(ERROR) << "ack_receiver::get_dma_address() returned no address.";
        [[maybe_unused]] auto const shutdown_result = ack_receiver_->shutdown();
        ack_receiver_.reset();
        return std::nullopt;
    }
    return dma_address;
}

bool datastore_impl::shutdown_rdma_ack_receiver() noexcept {
    if (! ack_receiver_) {
        return true;
    }

    auto result = ack_receiver_->shutdown();
    if (! result.success) {
        LOG_LP(ERROR) << "ack_receiver::shutdown() failed: " << result.error_message;
        return false;
    }

    ack_receiver_.reset();
    return true;
}

void datastore_impl::set_rdma_sender_for_test(std::unique_ptr<rdma_sender_base> sender) noexcept {
    rdma_sender_ = std::move(sender);
}

void datastore_impl::set_log_channel_connector_factory_for_test(
    std::function<std::unique_ptr<replication::replica_connector>()> factory) noexcept {
    log_channel_connector_factory_for_test_ = std::move(factory);
}

void datastore_impl::set_rdma_stream_factory_for_test(
        std::function<rdma_sender_base::stream_acquire_result(std::uint16_t)> factory) noexcept {
    rdma_stream_factory_for_test_ = std::move(factory);
}

void datastore_impl::set_rdma_control_send_stream_for_test(
        std::unique_ptr<rdma_send_stream_base> stream) noexcept {
    rdma_control_send_stream_ = std::move(stream);
}

void datastore_impl::set_handshake_connector_factory_for_test(
        handshake_connector_factory factory) noexcept {
    handshake_connector_factory_for_test_ = std::move(factory);
}

void datastore_impl::set_rdma_ack_receiver_factory_for_test(
        rdma_ack_receiver_factory factory) noexcept {
    rdma_ack_receiver_factory_for_test_ = std::move(factory);
}

void datastore_impl::set_rdma_data_sender_factory_for_test(
        rdma_data_sender_factory factory) noexcept {
    rdma_data_sender_factory_for_test_ = std::move(factory);
}

const std::optional<manifest::migration_info>& datastore_impl::get_migration_info() const noexcept {
    return migration_info_;
}

void datastore_impl::set_migration_info(const manifest::migration_info& info) noexcept {
    migration_info_ = info;
}

void datastore_impl::generate_hmac_secret_key() {
    // Generate 16 random bytes using OpenSSL RAND_bytes()
    // TODO: Future improvement - throw exception instead of abort when public API allows it
    if (RAND_bytes(hmac_secret_key_.data(), static_cast<int>(hmac_secret_key_.size())) != 1) {
        LOG_LP(ERROR) << "Failed to generate random bytes for BLOB access control secret key";
        std::abort(); // Current: abort due to noexcept constraint in public API
    }
}

const std::array<std::uint8_t, 16>& datastore_impl::get_hmac_secret_key() const noexcept {
    return hmac_secret_key_;
}

blob_reference_tag_type datastore_impl::generate_reference_tag(
        blob_id_type blob_id,
        std::uint64_t transaction_id) const {
    std::array<unsigned char, sizeof(blob_id_type) + sizeof(std::uint64_t)> input_bytes{};
    std::memcpy(input_bytes.data(), &blob_id, sizeof(blob_id_type));
    std::memcpy(input_bytes.data() + sizeof(blob_id_type),
            &transaction_id, sizeof(std::uint64_t));

    auto const& secret_key = get_hmac_secret_key();

    ERR_clear_error();

    std::array<unsigned char, EVP_MAX_MD_SIZE> md{};
    unsigned int md_len = 0;

    unsigned char* result = HMAC(EVP_sha256(),
            secret_key.data(),
            static_cast<int>(secret_key.size()),
            input_bytes.data(),
            input_bytes.size(),
            md.data(),
            &md_len);

    if (! result) {
        std::string msg = "Failed to calculate reference tag: ";
        // NOLINTNEXTLINE(google-runtime-int) : OpenSSL API requires unsigned long
        unsigned long openssl_err = 0;
        bool has_error = false;
        while ((openssl_err = ERR_get_error()) != 0) {
            has_error = true;
            std::array<char, 256> err_msg_buf{};
            ERR_error_string_n(openssl_err,
                    err_msg_buf.data(),
                    err_msg_buf.size());
            msg += "[" + std::to_string(openssl_err) + ": "
                    + err_msg_buf.data() + "] ";
        }
        if (! has_error) {
            msg += "No OpenSSL error code available.";
        }
        LOG_AND_THROW_BLOB_EXCEPTION_NO_ERRNO(msg);
    }

    blob_reference_tag_type tag = 0;
    std::memcpy(&tag, md.data(), sizeof(blob_reference_tag_type));

    return tag;
}

void datastore_impl::initialize_blob_file_resolver(boost::filesystem::path const& location) {
    blob_file_resolver_ = std::make_unique<limestone::internal::blob_file_resolver>(location);
}

limestone::internal::blob_file_resolver& datastore_impl::blob_file_resolver() noexcept {
    return require_blob_file_resolver();
}

limestone::internal::blob_file_resolver const& datastore_impl::blob_file_resolver() const noexcept {
    return require_blob_file_resolver();
}

boost::filesystem::path datastore_impl::resolve_blob_path(blob_id_type blob_id) const noexcept {
    return require_blob_file_resolver().resolve_path(blob_id);
}

limestone::internal::blob_file_resolver& datastore_impl::require_blob_file_resolver() noexcept {
    if (!blob_file_resolver_) {
        LOG_LP(FATAL) << "BLOB file resolver is not initialized.";
    }
    return *blob_file_resolver_;
}

limestone::internal::blob_file_resolver const& datastore_impl::require_blob_file_resolver() const noexcept {
    if (!blob_file_resolver_) {
        LOG_LP(FATAL) << "BLOB file resolver is not initialized.";
    }
    return *blob_file_resolver_;
}

}  // namespace limestone::api
