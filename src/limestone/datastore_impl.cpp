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

#include "blob_file_resolver.h"
#include "blob_file_scanner.h"
#include "compaction_catalog.h"
#include "limestone/logging.h"
#include "limestone_exception_helper.h"
#include "logging_helper.h"
#include "manifest.h"
#include "replication/message_group_commit.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_session_begin.h"
#include "replication/replica_connector.h"
#include "wal_sync/wal_history.h"

namespace limestone::api {
epoch_id_type datastore_impl::get_boot_durable_epoch_id() const noexcept {
    return boot_durable_epoch_id_.load(std::memory_order_seq_cst);
}

void datastore_impl::set_boot_durable_epoch_id(epoch_id_type epoch_id) noexcept {
    boot_durable_epoch_id_.store(epoch_id, std::memory_order_seq_cst);
}

using limestone::internal::compaction_catalog;
using limestone::internal::blob_file_scanner;

datastore_impl::datastore_impl(datastore& ds)
    : datastore_(ds)
    , backup_counter_(0)
    , replica_exists_(false)
    , async_session_close_enabled_(std::getenv("REPLICATION_ASYNC_SESSION_CLOSE") != nullptr)
    , async_group_commit_enabled_(std::getenv("REPLICATION_ASYNC_GROUP_COMMIT") != nullptr)
    , migration_info_(std::nullopt)
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
    VLOG_LP(log_debug) << "Checking if backup is in progress; active backup count: " << count;
    return count > 0;
}

// Returns true if a replica exists.
bool datastore_impl::has_replica() const noexcept {
    bool exists = replica_exists_.load(std::memory_order_acquire);
    VLOG_LP(log_debug) << "Checking replica existence; replica exists: " << exists;
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
            LOG_LP(ERROR) << "Failed to send group commit message to replica.";
            TRACE_END << "Failed to send group commit message.";
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

const std::optional<manifest::migration_info>& datastore_impl::get_migration_info() const noexcept {
    return migration_info_;
}

void datastore_impl::set_migration_info(const manifest::migration_info& info) noexcept {
    migration_info_ = info;
}

compaction_catalog& datastore_impl::get_compaction_catalog() noexcept{
    return *datastore_.compaction_catalog_;
}

backup_detail_and_rotation_result datastore_impl::begin_backup_with_rotation_result(backup_type btype) {  // NOLINT(readability-function-cognitive-complexity)
    datastore_.rotate_epoch_file();
    rotation_result result = datastore_.rotate_log_files();

    // LOG-0: all files are log file, so all files are selected in both standard/transaction mode.
    (void)btype;

    // calculate files_ minus active-files
    std::set<boost::filesystem::path> inactive_files(result.get_rotation_end_files());
    inactive_files.erase(datastore_.epoch_file_path_);
    for (const auto& lc : datastore_.log_channels_) {
        if (lc->registered_) {
            inactive_files.erase(lc->file_path());
        }
    }

    // build entries
    std::vector<backup_detail::entry> entries;
    for (auto& ent : inactive_files) {
        // LOG-0: assume files are located flat in logdir.
        std::string filename = ent.filename().string();
        auto dst = filename;
        switch (filename[0]) {
            case 'p': {
                if (filename.find("wal", 1) == 1) {
                    // "pwal"
                    // pwal files are type:logfile, detached

                    // skip an "inactive" file with the name of active file,
                    // it will cause some trouble if a file (that has the name of mutable files) is saved as immutable file.
                    // but, by skip, backup files may be imcomplete.
                    if (filename.length() == 9) {  // FIXME: too adohoc check
                        boost::system::error_code error;
                        bool result = boost::filesystem::is_empty(ent, error);
                        if (!error && !result) {
                            LOG_LP(ERROR) << "skip the file with the name like active files: " << filename;
                        }
                        continue;
                    }
                    entries.emplace_back(ent.string(), dst, false, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'e': {
                if (filename.find("poch", 1) == 1) {
                    // "epoch"
                    // epoch file(s) are type:logfile, the last rotated file is non-detached

                    // skip active file
                    if (filename.length() == 5) {  // FIXME: too adohoc check
                        continue;
                    }

                    // TODO: only last epoch file is not-detached
                    entries.emplace_back(ent.string(), dst, false, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'l': {
                if (filename == internal::manifest::file_name) {
                    entries.emplace_back(ent.string(), dst, true, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'c': {
                if (filename == compaction_catalog::get_catalog_filename()) {
                    entries.emplace_back(ent.string(), dst, false, false);
                }
                break;
            }
            case 'w': {
                if (filename == internal::wal_history::file_name()) {
                    entries.emplace_back(ent.string(), dst, false, false);
                }
                break;
            }
            default: {
                // unknown type
            }
        }
    }
    // Add blob files to the backup target
    blob_file_scanner scanner(datastore_.blob_file_resolver_.get());
    // Use the parent of the blob root as the base for computing the relative path.
    boost::filesystem::path backup_root = datastore_.blob_file_resolver_->get_blob_root().parent_path();
    for (const auto& src : scanner) {
        entries.emplace_back(src, src.filename(), false, false);
    }
    auto epoch_id = static_cast<uint64_t>(datastore_.epoch_id_switched_.load());
    auto backup_detail_ptr = std::unique_ptr<backup_detail>(new backup_detail(entries, epoch_id, *this));
    return {std::move(backup_detail_ptr), result};
}

}  // namespace limestone::api
