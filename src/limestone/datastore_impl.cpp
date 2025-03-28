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
#include "datastore_impl.h"

namespace limestone::api {

// Default constructor initializes the backup counter to zero.
datastore_impl::datastore_impl()
    : backup_counter_(0), replica_exists_(false) {
    bool has_replica = replication_endpoint_.is_valid();
    replica_exists_.store(has_replica, std::memory_order_release);
    LOG_LP(INFO) << "Replica " << (has_replica ? "enabled" : "disabled")
                    << "; endpoint valid: " << replication_endpoint_.is_valid();
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
    VLOG_LP(log_info) << "Checking if backup is in progress; active backup count: " << count;
    return count > 0;
}

// Returns true if a replica exists.
bool datastore_impl::has_replica() const noexcept {
    bool exists = replica_exists_.load(std::memory_order_acquire);
    VLOG_LP(log_info) << "Checking replica existence; replica exists: " << exists;
    return exists;
}

// Disables the replica.
void datastore_impl::disable_replica() noexcept {
    replica_exists_.store(false, std::memory_order_release);
    LOG_LP(INFO) << "Replica disabled";
}

// Method to open the control channel
bool datastore_impl::open_control_channel() {
    TRACE_START;
    // Use replication_endpoint_ to retrieve connection details
    if (!replication_endpoint_.is_valid()) {
        LOG_LP(ERROR) << "Invalid replication endpoint.";
        replica_exists_.store(false, std::memory_order_release);
        return false;
    }

    std::string host = replication_endpoint_.host();  // Get the host
    int port = replication_endpoint_.port();          // Get the port

    // Create the control channel connection
    control_channel_ = std::make_shared<replica_connector>();
    if (!control_channel_->connect_to_server(host, port)) {
        LOG_LP(ERROR) << "Failed to connect to control channel at " << host << ":" << port;
        replica_exists_.store(false, std::memory_order_release);
        return false;
    }

    auto request = message_session_begin::create();
    if (!control_channel_->send_message(*request)) {
        LOG_LP(ERROR) << "Failed to send session begin message.";
        control_channel_->close_session();
        return false;
    }

    auto response = control_channel_->receive_message();
    if (response == nullptr || response->get_message_type_id() != message_type_id::SESSION_BEGIN_ACK) {
        LOG_LP(ERROR) << "Failed to receive session begin acknowledgment.";
        control_channel_->close_session();
        return false;
    }

    LOG_LP(INFO) << "Control channel successfully opened to " << host << ":" << port;
    TRACE_END;
    return true;
}

bool datastore_impl::is_replication_configured() const noexcept {
    return replication_endpoint_.env_defined();
}

// Getter for control_channel_
std::shared_ptr<replica_connector> datastore_impl::get_control_channel() const noexcept {
    return control_channel_;
}

}  // namespace limestone::api
