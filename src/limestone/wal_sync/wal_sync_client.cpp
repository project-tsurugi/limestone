/*
 * Copyright 2023-2025 Project Tsurugi.
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

#include <wal_sync/wal_sync_client.h>

#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <grpc/client/backup_client.h>
#include <atomic>
#include <sstream>
#include <thread>
#include <wal_sync/response_chunk_processor.h>
#include "file_operations.h"
#include "manifest.h"
#include "dblog_scan.h"
#include "wal_history.grpc.pb.h"
#include "grpc/service/message_versions.h"
#include "grpc/service/grpc_constants.h"
#include "wal_history.h"
namespace limestone::internal {

using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::proto::GetObjectRequest;
using limestone::grpc::proto::GetObjectResponse;
using limestone::grpc::proto::KeepAliveRequest;
using limestone::grpc::proto::KeepAliveResponse;
using limestone::grpc::proto::EndBackupRequest;
using limestone::grpc::proto::EndBackupResponse;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::list_wal_history_message_version;
using limestone::grpc::service::grpc_timeout_ms;
using limestone::grpc::service::get_object_message_version;
using limestone::grpc::service::keep_alive_message_version;
using limestone::grpc::service::end_backup_message_version;
using limestone::grpc::service::keepalive_interval_ms;

using limestone::internal::response_chunk_processor;

bool wal_sync_client::init(std::string& error_message, bool allow_initialize) {

    // Check if log_dir_ exists and is a directory
    if (!boost::filesystem::exists(log_dir_)) {
        if (allow_initialize) {
            boost::system::error_code ec;
            if (!boost::filesystem::create_directory(log_dir_, ec) || ec) {
                error_message = "failed to create log_dir: " + log_dir_.string() + ", " + ec.message();
                return false;
            }
        } else {
            error_message = "log_dir does not exist: " + log_dir_.string();
            return false;
        }
    } else if (!boost::filesystem::is_directory(log_dir_)) {
        error_message = "log_dir is not a directory: " + log_dir_.string();
        return false;
    } else if (boost::filesystem::directory_iterator(log_dir_) == boost::filesystem::directory_iterator()) {
        // Directory exists but is empty
        if (!allow_initialize) {
            error_message = "log_dir is empty: " + log_dir_.string();
            return false;
        }
    }

    // If directory was just created or is empty and allow_initialize, create manifest
    if (allow_initialize && (
            !boost::filesystem::exists(log_dir_)
            || boost::filesystem::directory_iterator(log_dir_) == boost::filesystem::directory_iterator())) {
        manifest::create_initial(log_dir_);
    }

    real_file_operations ops;
    boost::filesystem::path manifest_path = log_dir_ / std::string(manifest::file_name);
    auto manifest_opt = manifest::load_manifest_from_path(manifest_path, ops);
    if (!manifest_opt) {
        error_message = "manifest file not found or invalid: " + manifest_path.string();
        return false;
    }
    const std::string& format_version = manifest_opt->get_format_version();
    if (format_version != manifest::default_format_version) {
        error_message = "unsupported manifest format_version: '" + format_version +
            "' (expected: '" + std::string(manifest::default_format_version) + "')";
        return false;
    }
    int persistent_version = manifest_opt->get_persistent_format_version();
    if (persistent_version != manifest::default_persistent_format_version) {
        error_message = "unsupported manifest persistent_format_version: " + std::to_string(persistent_version) +
            " (expected: " + std::to_string(manifest::default_persistent_format_version) + ")";
        return false;
    }

    // Acquire manifest lock (will be released in destructor)
    lock_fd_ = manifest::acquire_lock(log_dir_, *file_ops_);
    if (lock_fd_ < 0) {
        error_message = "failed to acquire manifest lock: " + log_dir_.string();
        return false;
    }
    return true;
}

// destructor: release manifest lock if held
wal_sync_client::~wal_sync_client() {
    if (lock_fd_ >= 0 && file_ops_) {
        file_ops_->close(lock_fd_);
        lock_fd_ = -1;
    }
}


wal_sync_client::wal_sync_client(boost::filesystem::path log_dir, std::shared_ptr<::grpc::Channel> const& channel) noexcept
    : log_dir_(std::move(log_dir))
    , real_file_ops_()
    , file_ops_(&real_file_ops_)
    , history_client_(std::make_shared<limestone::grpc::client::wal_history_client>(channel))
    , backup_client_(std::make_shared<limestone::grpc::client::backup_client>(channel)) {}





std::optional<epoch_id_type> wal_sync_client::get_remote_epoch() {
    WalHistoryResponse response;
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    ::grpc::Status status = history_client_->get_wal_history(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        LOG(ERROR) << "get_remote_epoch failed: "
                   << status.error_code() << " / " << status.error_message();
        return std::nullopt;
    }
    return response.last_epoch();
}

epoch_id_type wal_sync_client::get_local_epoch() {
    dblog_scan scan(log_dir_);
    return scan.last_durable_epoch_in_dir();
}

std::optional<std::vector<branch_epoch>> wal_sync_client::get_remote_wal_compatibility() {
    WalHistoryResponse response;
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    ::grpc::Status status = history_client_->get_wal_history(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        LOG(ERROR) << "get_remote_wal_compatibility failed: "
                   << status.error_code() << " / " << status.error_message();
        return std::nullopt;
    }
    std::vector<branch_epoch> result;
    result.reserve(response.records_size());
    for (const auto& record : response.records()) {
        result.emplace_back(branch_epoch{
            record.epoch(),
            record.identity(),
            record.timestamp()
        });
    }
    return result;
}

std::vector<branch_epoch> wal_sync_client::get_local_wal_compatibility() {
    wal_history wal_history_(log_dir_);
    auto records = wal_history_.list();
    std::vector<branch_epoch> result;
    result.reserve(records.size());
    for (const auto& record : records) {
        result.emplace_back(branch_epoch{
            record.epoch,
            record.identity,
            record.timestamp
        });
    }
    return result;
}

bool wal_sync_client::check_wal_compatibility(
    std::vector<branch_epoch> const& local,
    std::vector<branch_epoch> const& remote
) {
    if (local.empty() || remote.empty() || local.size() > remote.size()) {
        return false;
    }

    for (std::size_t i = 0; i < local.size(); ++i) {
        if (local[i].epoch != remote[i].epoch ||
            local[i].identity != remote[i].identity ||
            local[i].timestamp != remote[i].timestamp) {
            return false;
        }
    }
    return true;
}

std::optional<begin_backup_result> wal_sync_client::begin_backup(
    std::uint64_t begin_epoch,
    std::uint64_t end_epoch
) {
    BeginBackupRequest request;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(begin_epoch);
    request.set_end_epoch(end_epoch);

    BeginBackupResponse response;
    ::grpc::Status status = backup_client_->begin_backup(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        LOG(ERROR) << "begin_backup failed: "
                   << status.error_code() << " / " << status.error_message();
        return std::nullopt;
    }

    begin_backup_result result{};
    result.session_token = response.session_id();
    result.expire_at = std::chrono::system_clock::time_point{std::chrono::seconds{response.expire_at()}};
    result.objects.reserve(static_cast<std::size_t>(response.objects_size()));
    for (auto const& object : response.objects()) {
        result.objects.emplace_back(backup_object{
            object.object_id(),
            backup_object_type_helper::from_proto(object.type()),
            object.path()
        });
    }
    return result;
}

remote_backup_result wal_sync_client::copy_backup_objects(
    std::string const& session_token,
    std::vector<backup_object> const& objects,
    boost::filesystem::path const& output_dir
) {
    remote_backup_result result{};

    if (objects.empty()) {
        result.success = true;
        return result;
    }

    boost::system::error_code dir_ec;
    file_ops_->create_directories(output_dir, dir_ec);
    if (dir_ec) {
        std::ostringstream oss;
        oss << "failed to prepare output directory: " << output_dir.string()
            << ", ec=" << dir_ec.message();
        result.error_message = oss.str();
        LOG(ERROR) << result.error_message;
        return result;
    }

    response_chunk_processor processor(*file_ops_, output_dir, objects);

    auto handler = [&processor](GetObjectResponse const& response) {
        processor.handle_response(response);
    };

    GetObjectRequest request;
    request.set_version(get_object_message_version);
    request.set_session_id(session_token);
    for (auto const& object : objects) {
        request.add_object_id(object.id);
    }

    ::grpc::Status status = backup_client_->get_object(request, handler, grpc_timeout_ms);
    if (!status.ok()) {
        std::ostringstream oss;
        oss << "get_object RPC failed: " << status.error_code()
            << " / " << status.error_message();
        result.error_message = oss.str();
        LOG(ERROR) << result.error_message;
        processor.cleanup_partials();
        return result;
    }

    if (processor.failed()) {
        result.error_message = processor.error_message();
        LOG(ERROR) << "failed to copy backup objects: " << result.error_message;
        processor.cleanup_partials();
        return result;
    }

    if (!processor.all_completed()) {
        auto incomplete = processor.incomplete_object_ids();
        for (auto const& id : incomplete) {
            LOG(ERROR) << "copy incomplete for object_id: " << id;
        }
        result.error_message = "copy incomplete for one or more objects";
        result.incomplete_object_ids = std::move(incomplete);
        processor.cleanup_partials();
        return result;
    }

    result.success = true;
    return result;
}

bool wal_sync_client::keepalive_session(std::string const& session_token) {
    KeepAliveRequest request;
    request.set_version(keep_alive_message_version);
    request.set_session_id(session_token);

    KeepAliveResponse response;
    ::grpc::Status status = backup_client_->keep_alive(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        LOG(ERROR) << "keep_alive RPC failed: " << status.error_code()
                   << " / " << status.error_message();
        return false;
    }
    return true;
}

bool wal_sync_client::end_backup(std::string const& session_token) {
    EndBackupRequest request;
    request.set_version(end_backup_message_version);
    request.set_session_id(session_token);

    EndBackupResponse response;
    ::grpc::Status status = backup_client_->end_backup(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        LOG(ERROR) << "end_backup RPC failed: " << status.error_code()
                   << " / " << status.error_message();
        return false;
    }
    return true;
}

remote_backup_result wal_sync_client::execute_remote_backup(
    std::uint64_t begin_epoch,
    std::uint64_t end_epoch,
    boost::filesystem::path const& output_dir
) {
    remote_backup_result result{};

    auto begin_result_opt = begin_backup(begin_epoch, end_epoch);
    if (!begin_result_opt) {
        result.error_message = "begin_backup failed";
        return result;
    }
    begin_backup_result begin_result = std::move(*begin_result_opt);

    std::atomic<bool> keepalive_running{true};
    std::atomic<bool> keepalive_failed{false};
    std::thread keepalive_thread;

    struct keepalive_thread_guard final {
    public:
        keepalive_thread_guard(std::atomic<bool>& running, std::thread& thread)
            : running_(running)
            , thread_(thread) {}

        keepalive_thread_guard(keepalive_thread_guard const&) = delete;
        keepalive_thread_guard& operator=(keepalive_thread_guard const&) = delete;
        keepalive_thread_guard(keepalive_thread_guard&&) = delete;
        keepalive_thread_guard& operator=(keepalive_thread_guard&&) = delete;

        ~keepalive_thread_guard() {
            if (!dismissed_) {
                running_.store(false, std::memory_order_relaxed);
                if (thread_.joinable()) {
                    thread_.join();
                }
            }
        }

        void dismiss() {
            dismissed_ = true;
        }

    private:
        std::atomic<bool>& running_;
        std::thread& thread_;
        bool dismissed_ = false;
    } keepalive_guard{keepalive_running, keepalive_thread};

    std::chrono::milliseconds keepalive_interval{keepalive_interval_ms};
    if (keepalive_interval.count() > 0 && !begin_result.session_token.empty()) {
        keepalive_thread = std::thread([
            this,
            session_token = begin_result.session_token,
            keepalive_interval,
            &keepalive_running,
            &keepalive_failed
        ]() {
            while (keepalive_running.load(std::memory_order_relaxed)) {
                if (!keepalive_session(session_token)) {
                    LOG(ERROR) << "keepalive_session failed during execute_remote_backup";
                    keepalive_failed.store(true, std::memory_order_relaxed);
                    break;
                }
                auto elapsed = std::chrono::milliseconds{0};
                while (keepalive_running.load(std::memory_order_relaxed) && elapsed < keepalive_interval) {
                    auto step = std::chrono::milliseconds{50};
                    std::this_thread::sleep_for(step);
                    elapsed += step;
                }
            }
        });
    }

    remote_backup_result copy_result = copy_backup_objects(begin_result.session_token, begin_result.objects, output_dir);

    keepalive_running.store(false, std::memory_order_relaxed);
    if (keepalive_thread.joinable()) {
        keepalive_thread.join();
    }
    keepalive_guard.dismiss();

    remote_backup_result final_result = copy_result;

    if (keepalive_failed.load(std::memory_order_relaxed)) {
        if (final_result.success) {
            final_result.success = false;
            final_result.error_message = "keepalive_session failed during remote backup";
        } else if (final_result.error_message.empty()) {
            final_result.error_message = "keepalive_session failed during remote backup";
        }
    }

    bool end_result = end_backup(begin_result.session_token);
    if (!end_result) {
        if (final_result.success) {
            final_result.success = false;
            final_result.error_message = "end_backup RPC failed";
        } else if (final_result.error_message.empty()) {
            final_result.error_message = "end_backup RPC failed";
        }
    }

    return final_result;
}

bool wal_sync_client::deploy_objects(std::vector<backup_object> const& objects) {
    (void)objects;
    // TODO: implement
    return false;
}

bool wal_sync_client::compact_wal() {
    // TODO: implement
    return false;
}

void wal_sync_client::set_file_operations(file_operations& file_ops) {
    file_ops_ = &file_ops;
}

} // namespace limestone::internal
