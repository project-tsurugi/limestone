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

#include <cstdlib>
#include <iostream>

#include "limestone/logging.h"
#include "limestone_exception_helper.h"
#include "logging_helper.h"
#include "now_nsec.h"
#include "replication/message_group_commit.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_session_begin.h"
#include "replication/replica_connector.h"

namespace limestone::api {

// Default constructor initializes the backup counter to zero.
datastore_impl::datastore_impl()
    : backup_counter_(0)
    , replica_exists_(false)
    , async_session_close_enabled_(std::getenv("REPLICATION_ASYNC_SESSION_CLOSE") != nullptr)
    , async_group_commit_enabled_(std::getenv("REPLICATION_ASYNC_GROUP_COMMIT") != nullptr)
{
    LOG_LP(INFO) << "REPLICATION_ASYNC_SESSION_CLOSE: "
                 << (async_session_close_enabled_ ? "enabled" : "disabled");
    LOG_LP(INFO) << "REPLICATION_ASYNC_GROUP_COMMIT: "
                 << (async_group_commit_enabled_ ? "enabled" : "disabled");

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

    std::string host = replication_endpoint_.host();  
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
        replica_exists_.store(false, std::memory_order_release);
        control_channel_->close_session();
        return false;
    }

    auto response = control_channel_->receive_message();
    if (response == nullptr || response->get_message_type_id() != message_type_id::SESSION_BEGIN_ACK) {
        LOG_LP(ERROR) << "Failed to receive session begin acknowledgment.";
        replica_exists_.store(false, std::memory_order_release);
        control_channel_->close_session();
        return false;
    }

    LOG_LP(INFO) << "Control channel successfully opened to " << host << ":" << port;
    TRACE_END;
    return true;
}

bool datastore_impl::propagate_group_commit(uint64_t epoch_id) {
    if (!is_master_) {
        return false;
    }
    if (replica_exists_.load(std::memory_order_acquire)) {
        TRACE_START << "epoch_id=" << epoch_id;
        message_group_commit message{epoch_id};
        if (!control_channel_->send_message(message)) {
            LOG_LP(ERROR) << "Failed to send switch epoch message to replica.";
            TRACE_END << "Failed to send switch epoch message.";
            return false;
        }
        TRACE_END;
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

std::unique_ptr<replication::replica_connector> datastore_impl::create_log_channel_connector(datastore& ds) {
    TRACE_START;
    if (!replica_exists_.load(std::memory_order_acquire)) {
        TRACE_END << "No replica exists, cannot create log channel connector.";
        return nullptr;
    }
    auto connector = std::make_unique<replica_connector>();

    std::string host = replication_endpoint_.host();  
    int port = replication_endpoint_.port();          
    if (!connector->connect_to_server(host, port, ds)) {  
        LOG_LP(ERROR) << "Failed to connect to control channel at " << host << ":" << port;
        replica_exists_.store(false, std::memory_order_release);
        return nullptr;
    }

    auto request = message_log_channel_create::create();
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

}  // namespace limestone::api
