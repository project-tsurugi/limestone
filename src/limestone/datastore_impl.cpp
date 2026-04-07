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
#include <replication/message_rdma_init.h>
#include <replication/message_rdma_init_ack.h>
#include <rdma/rdma_factory.h>
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

bool datastore_impl::maybe_initialize_rdma_sender() {
    if (!rdma_slot_count_.has_value()) {
        return true;
    }

    auto slot_count_signed = static_cast<std::int32_t>(rdma_slot_count_.value());
    // This branch should be unreachable because slots are validated on load, but kept for defense.
    if (slot_count_signed <= 0) {
        LOG_LP(ERROR) << "Invalid RDMA slot count detected in runtime state; RDMA disabled.";
        return true;
    }

    auto slot_count = static_cast<uint32_t>(slot_count_signed);
    message_rdma_init rdma_init{slot_count};
    if (!control_channel_->send_message(rdma_init)) {
        LOG_LP(ERROR) << "Failed to send RDMA_INIT message.";
        return false;
    }

    auto rdma_response = control_channel_->receive_message();
    if (rdma_response == nullptr) {
        LOG_LP(ERROR) << "Failed to receive RDMA_INIT response.";
        return false;
    }

    if (rdma_response->get_message_type_id() == message_type_id::COMMON_ERROR) {
        auto* err = dynamic_cast<message_error*>(rdma_response.get());
        if (err != nullptr) {
            LOG_LP(ERROR) << "RDMA_INIT failed: code=" << err->get_error_code()
                          << " message=" << err->get_error_message();
        } else {
            LOG_LP(ERROR) << "RDMA_INIT failed with unknown error response.";
        }
        return false;
    }

    auto* ack = dynamic_cast<message_rdma_init_ack*>(rdma_response.get());
    if (ack == nullptr) {
        LOG_LP(ERROR) << "Unexpected RDMA_INIT response type: "
                      << static_cast<uint16_t>(rdma_response->get_message_type_id());
        return false;
    }
    if (!initialize_rdma_sender(slot_count, ack->get_remote_dma_address())) {
        LOG_LP(ERROR) << "RDMA sender initialization failed; RDMA disabled.";
        return false;
    }
    LOG_LP(INFO) << "RDMA sender initialized: slot_count=" << slot_count
                 << ", remote_dma_address=" << ack->get_remote_dma_address();
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

    if (!maybe_initialize_rdma_sender()) {
        TRACE_END;
        return false;
    }
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

void datastore_impl::wait_for_propagated_group_commit_ack() {
    TRACE_START;
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
    return replication_endpoint_.env_defined();
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

bool datastore_impl::initialize_rdma_sender(uint32_t slot_count, uint64_t remote_dma_address) {
    rdma_sender_ = make_rdma_sender(slot_count);
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

bool datastore_impl::shutdown_rdma_sender() noexcept {
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

void datastore_impl::set_rdma_sender_for_test(std::unique_ptr<rdma_sender_base> sender) noexcept {
    rdma_sender_ = std::move(sender);
}

void datastore_impl::set_log_channel_connector_factory_for_test(
    std::function<std::unique_ptr<replication::replica_connector>()> factory) noexcept {
    log_channel_connector_factory_for_test_ = std::move(factory);
}

void datastore_impl::set_rdma_stream_factory_for_test(
        std::function<rdma_sender_base::stream_acquire_result(std::uint16_t, int)> factory) noexcept {
    rdma_stream_factory_for_test_ = std::move(factory);
}

std::function<rdma_sender_base::stream_acquire_result(std::uint16_t, int)> const*
datastore_impl::get_rdma_stream_factory_for_test() const noexcept {
    if (rdma_stream_factory_for_test_) {
        return &rdma_stream_factory_for_test_;
    }
    return nullptr;
}

void datastore_impl::set_rdma_ack_fd_for_test(int fd) noexcept {
    rdma_ack_fd_for_test_ = fd;
}

bool datastore_impl::has_rdma_stream_factory_for_test() const noexcept {
    return static_cast<bool>(rdma_stream_factory_for_test_);
}

std::optional<int> datastore_impl::rdma_ack_fd_for_test() const noexcept {
    return rdma_ack_fd_for_test_;
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

}  // namespace limestone::api
