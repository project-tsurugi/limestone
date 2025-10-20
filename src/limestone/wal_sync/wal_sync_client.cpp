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

#include <glog/logging.h>
#include <grpc/client/backup_client.h>
#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <limestone/status.h>
#include <wal_sync/response_chunk_processor.h>
#include <wal_sync/wal_sync_client.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <condition_variable>
#include <cerrno>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <cstring>
#include <ctime>
#include <thread>
#include <vector>

#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "dblog_scan.h"
#include "file_operations.h"
#include "grpc/service/grpc_constants.h"
#include "grpc/service/message_versions.h"
#include "internal.h"
#include "manifest.h"
#include "log_entry.h"
#include "wal_history.grpc.pb.h"
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

rotation_aware_datastore::rotation_aware_datastore(limestone::api::configuration const& conf)
    : limestone::api::datastore(conf) {}

void rotation_aware_datastore::set_rotation_handler(std::function<void()> handler) {
    rotation_handler_ = std::move(handler);
}

void rotation_aware_datastore::on_rotate_log_files() noexcept {
    if (rotation_handler_) {
        rotation_handler_();
    }
}

void rotation_aware_datastore::trigger_rotation_handler_for_tests() {
    if (rotation_handler_) {
        rotation_handler_();
    }
}

void rotation_aware_datastore::perform_compaction() {
    compact_with_online();
}

void rotation_aware_datastore::perform_switch_epoch(epoch_id_type value) {
    switch_epoch(value);
}

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

    return true;
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
    result.finish_epoch = response.finish_epoch();
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

namespace {

class keepalive_thread_guard final {
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
};

} // namespace

std::unique_ptr<rotation_aware_datastore> wal_sync_client::create_rotation_aware_datastore() {
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(log_dir_);
    limestone::api::configuration conf(data_locations, log_dir_);
    return std::make_unique<rotation_aware_datastore>(conf);
}

std::pair<epoch_id_type, bool> wal_sync_client::prepare_for_compaction(
    rotation_aware_datastore& datastore,
    std::atomic<bool>& rotation_triggered,
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex
) {
    (void)rotation_mutex;
    datastore.set_rotation_handler([&rotation_triggered, &rotation_cv]() noexcept {
        rotation_triggered.store(true, std::memory_order_relaxed);
        rotation_cv.notify_one();
    });

    // Open datastore and obtain the last durable epoch as the baseline for incremental restore.
    epoch_id_type current_epoch = 0;
    try {
        ready_datastore(datastore);
        current_epoch = query_last_epoch(datastore);
    } catch (std::exception const& ex) {
        LOG(ERROR) << "failed to prepare datastore before incremental restore: " << ex.what();
        datastore.set_rotation_handler({});
        return {0, false};
    } catch (...) {
        LOG(ERROR) << "failed to prepare datastore before incremental restore due to unknown error";
        datastore.set_rotation_handler({});
        return {0, false};
    }

    if (current_epoch == 0) {
        LOG(ERROR) << "incremental restore aborted: last_epoch is 0 (log directory may be corrupt)";
        datastore.set_rotation_handler({});
        return {0, false};
    }

    return {current_epoch, true};
}

void wal_sync_client::ready_datastore(rotation_aware_datastore& datastore) {
    datastore.ready();
}

epoch_id_type wal_sync_client::query_last_epoch(rotation_aware_datastore const& datastore) const {
    return datastore.last_epoch();
}

std::thread wal_sync_client::launch_compaction_thread(
    rotation_aware_datastore& datastore,
    std::exception_ptr& compaction_error,
    std::atomic<bool>& /*rotation_triggered*/,
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex,
    bool& compaction_done
) {
    return std::thread([
        &datastore,
        &compaction_error,
        &rotation_cv,
        &rotation_mutex,
        &compaction_done
    ]() {
        std::exception_ptr thread_error{};
        try {
            datastore.perform_compaction();
        } catch (...) {
            thread_error = std::current_exception();
        }
        {
            std::lock_guard<std::mutex> lock(rotation_mutex);
            compaction_done = true;
            if (thread_error) {
                compaction_error = thread_error;
            }
        }
        rotation_cv.notify_one();
    });
}

bool wal_sync_client::wait_for_rotation_or_completion(
    std::atomic<bool>& rotation_triggered,
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex,
    bool& compaction_done,
    std::exception_ptr& compaction_error
) {
    std::unique_lock<std::mutex> lock(rotation_mutex);
    rotation_cv.wait(lock, [&]() {
        return rotation_triggered.load(std::memory_order_relaxed)
            || compaction_done
            || static_cast<bool>(compaction_error);
    });
    return rotation_triggered.load(std::memory_order_relaxed);
}

bool wal_sync_client::handle_rotation_after_trigger(
    rotation_aware_datastore& datastore,
    epoch_id_type current_epoch,
    std::atomic<bool>& rotation_triggered,
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex,
    bool& compaction_done,
    std::exception_ptr& compaction_error
) {
    if (!rotation_triggered.load(std::memory_order_relaxed)) {
        return true;
    }

    bool switch_failed = false;
    try {
        datastore.perform_switch_epoch(static_cast<epoch_id_type>(current_epoch + 1));
    } catch (std::exception const& ex) {
        LOG(ERROR) << "failed to switch epoch during incremental restore: " << ex.what();
        switch_failed = true;
    } catch (...) {
        LOG(ERROR) << "failed to switch epoch during incremental restore due to unknown error";
        switch_failed = true;
    }

    if (!switch_failed) {
        return true;
    }

    std::unique_lock<std::mutex> lock(rotation_mutex);
    rotation_cv.wait(lock, [&]() {
        return compaction_done || static_cast<bool>(compaction_error);
    });
    return false;
}

bool wal_sync_client::wait_for_compaction_completion(
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex,
    bool& compaction_done,
    std::exception_ptr& compaction_error
) {
    std::unique_lock<std::mutex> lock(rotation_mutex);
    rotation_cv.wait(lock, [&]() {
        return compaction_done || static_cast<bool>(compaction_error);
    });
    return !static_cast<bool>(compaction_error);
}

bool wal_sync_client::run_compaction_with_rotation(
    rotation_aware_datastore& datastore,
    epoch_id_type current_epoch,
    std::atomic<bool>& rotation_triggered,
    std::condition_variable& rotation_cv,
    std::mutex& rotation_mutex,
    std::exception_ptr& compaction_error
) {
    compaction_error = nullptr;
    bool compaction_done = false;

    std::thread compaction_thread = launch_compaction_thread(
        datastore,
        compaction_error,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_done
    );

    auto join_thread = [&]() {
        if (compaction_thread.joinable()) {
            compaction_thread.join();
        }
    };

    (void)wait_for_rotation_or_completion(
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_done,
        compaction_error
    );

    if (compaction_error) {
        join_thread();
        try {
            std::rethrow_exception(compaction_error);
        } catch (std::exception const& ex) {
            LOG(ERROR) << "compact_with_online failed: " << ex.what();
        } catch (...) {
            LOG(ERROR) << "compact_with_online failed with unknown error";
        }
        return false;
    }

    if (!handle_rotation_after_trigger(
            datastore,
            current_epoch,
            rotation_triggered,
            rotation_cv,
            rotation_mutex,
            compaction_done,
            compaction_error)) {
        join_thread();
        return false;
    }

    if (!wait_for_compaction_completion(rotation_cv, rotation_mutex, compaction_done, compaction_error)) {
        join_thread();
        try {
            std::rethrow_exception(compaction_error);
        } catch (std::exception const& ex) {
            LOG(ERROR) << "compact_with_online failed: " << ex.what();
        } catch (...) {
            // NOTE: This branch is intentionally left without dedicated test coverage.
            // It only executes when the compaction thread reports an error after rotation
            // has already unblocked the main thread, which is difficult to reproduce deterministically.
            LOG(ERROR) << "compact_with_online failed with unknown error";
        }
        return false;
    }

    join_thread();

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

    bool const is_fullbackup = (begin_epoch == 0 && end_epoch == 0);
    if (!is_fullbackup) {
        auto remote_history = get_remote_wal_compatibility();
        if (!remote_history) {
            result.error_message = "failed to retrieve remote wal_history";
            (void)end_backup(begin_result.session_token);
            return result;
        }
        std::string history_error;
        if (!write_wal_history_snapshot(*remote_history, begin_result.finish_epoch, output_dir, history_error)) {
            result.error_message = std::move(history_error);
            (void)end_backup(begin_result.session_token);
            return result;
        }
        std::string epoch_error;
        if (!write_epoch_marker(output_dir, begin_result.finish_epoch, epoch_error)) {
            result.error_message = std::move(epoch_error);
            (void)end_backup(begin_result.session_token);
            return result;
        }
    }

    
    std::atomic<bool> keepalive_running{true};
    std::thread keepalive_thread;

    remote_backup_result copy_result{};
    {
        keepalive_thread_guard keepalive_guard{keepalive_running, keepalive_thread};

        std::chrono::milliseconds keepalive_interval{keepalive_interval_ms};
        if (keepalive_interval.count() > 0 && !begin_result.session_token.empty()) {
            keepalive_thread = std::thread([
                this,
                session_token = begin_result.session_token,
                keepalive_interval,
                &keepalive_running
            ]() {
                while (keepalive_running.load(std::memory_order_relaxed)) {
                    if (!keepalive_session(session_token)) {
                        LOG(ERROR) << "keepalive_session failed during execute_remote_backup";
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

        copy_result = copy_backup_objects(begin_result.session_token, begin_result.objects, output_dir);

        keepalive_running.store(false, std::memory_order_relaxed);
    }

    remote_backup_result final_result = copy_result;

    (void)end_backup(begin_result.session_token);

    return final_result;
}

bool wal_sync_client::restore(
    std::uint64_t begin_epoch,
    std::uint64_t end_epoch,
    boost::filesystem::path const& output_dir
) {
    bool const is_full_restore = begin_epoch == 0 && end_epoch == 0;

    if (!is_full_restore) {
        if (compact_wal()) {
            LOG(INFO) << "WAL compaction completed successfully before incremental restore.";
        } else {
            LOG(ERROR) << "WAL compaction failed before incremental restore.";
            return false;
        }
    }
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(log_dir_);
    limestone::api::configuration conf(data_locations, log_dir_);
    limestone::api::datastore datastore_instance(conf);


    limestone::status const restore_status = datastore_instance.restore(output_dir.string(), false, is_full_restore);
    if (restore_status != limestone::status::ok) {
        LOG(ERROR) << "restore failed: status=" << restore_status;
        return false;
    }
    return true;
}

bool wal_sync_client::compact_wal() {
    std::atomic<bool> rotation_triggered{false};
    std::mutex rotation_mutex;
    std::condition_variable rotation_cv;
    std::exception_ptr compaction_error{};
    // Before building a datastore instance, ensure any attached pwal files are
    // detached (renamed) so they become selectable for compaction. This mirrors
    // the behavior of the dblogutil repair flow.
    try {
        dblog_scan ds(log_dir_);
        ds.detach_wal_files();
    } catch (std::exception const& ex) {
        LOG(ERROR) << "failed to detach wal files before compaction: " << ex.what();
        return false;
    } catch (...) {
        LOG(ERROR) << "failed to detach wal files before compaction due to unknown error";
        return false;
    }

    // Build a dedicated datastore instance for compaction.
    auto datastore = create_rotation_aware_datastore();
    if (!datastore) {
        LOG(ERROR) << "failed to create datastore for compaction";
        return false;
    }

    // Prepare the datastore and obtain the epoch baseline.
    auto prepared = prepare_for_compaction(*datastore, rotation_triggered, rotation_cv, rotation_mutex);
    epoch_id_type current_epoch = prepared.first;
    bool prepared_successfully = prepared.second;
    if (!prepared_successfully) {
        return false;
    }

    bool const compaction_succeeded = run_compaction_with_rotation(
        *datastore,
        current_epoch,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    // Compaction failure (including exception) has already been logged.
    if (!compaction_succeeded) {
        return false;
    }

    // Cleanup detached pwals before shutting down the datastore. The
    // compaction_catalog is held by the datastore_impl and can be accessed
    // via datastore->get_impl()->get_compaction_catalog().
    try {
        cleanup_detached_pwals(*datastore);
    } catch (std::exception const& ex) {
        LOG(ERROR) << "cleanup_detached_pwals threw exception: " << ex.what();
    } catch (...) {
        LOG(ERROR) << "cleanup_detached_pwals threw unknown exception";
    }

    std::future<void> shutdown_future;
    try {
        shutdown_future = datastore->shutdown();
    } catch (std::exception const& ex) {
        LOG(ERROR) << "failed to shutdown datastore after compaction: " << ex.what();
        return false;
    } catch (...) {
        LOG(ERROR) << "failed to shutdown datastore after compaction due to unknown error";
        return false;
    }
    if (shutdown_future.valid()) {
        shutdown_future.wait();
    }

    return true;
}

bool wal_sync_client::write_epoch_marker(
    const boost::filesystem::path& output_dir,
    epoch_id_type epoch,
    std::string& error_message
) {
    boost::system::error_code dir_ec;
    file_ops_->create_directories(output_dir, dir_ec);
    if (dir_ec) {
        std::ostringstream oss;
        oss << "failed to prepare output directory: " << output_dir.string()
            << ", ec=" << dir_ec.message();
        error_message = oss.str();
        return false;
    }

    auto epoch_path = output_dir / std::string(limestone::internal::epoch_file_name);
    FILE* fp = file_ops_->fopen(epoch_path.string().c_str(), "wb");
    if (!fp) {
        std::ostringstream oss;
        oss << "failed to create epoch file: " << epoch_path.string()
            << ", errno=" << errno << " (" << std::strerror(errno) << ")";
        error_message = oss.str();
        return false;
    }

    limestone::api::log_entry::durable_epoch(fp, epoch);

    if (file_ops_->fflush(fp) != 0) {
        std::ostringstream oss;
        oss << "failed to flush epoch file: " << epoch_path.string()
            << ", errno=" << errno << " (" << std::strerror(errno) << ")";
        error_message = oss.str();
        file_ops_->fclose(fp);
        return false;
    }
    int fd = file_ops_->fileno(fp);
    if (fd >= 0 && file_ops_->fsync(fd) != 0) {
        std::ostringstream oss;
        oss << "failed to fsync epoch file: " << epoch_path.string()
            << ", errno=" << errno << " (" << std::strerror(errno) << ")";
        error_message = oss.str();
        file_ops_->fclose(fp);
        return false;
    }

    // Close file before returning success
    file_ops_->fclose(fp);
    return true;
}

bool wal_sync_client::write_wal_history_snapshot(
    const std::vector<branch_epoch>& remote_history,
    epoch_id_type finish_epoch,
    const boost::filesystem::path& output_dir,
    std::string& error_message
) {
    boost::system::error_code dir_ec;
    file_ops_->create_directories(output_dir, dir_ec);
    if (dir_ec) {
        std::ostringstream oss;
        oss << "failed to prepare output directory: " << output_dir.string()
            << ", ec=" << dir_ec.message();
        error_message = oss.str();
        return false;
    }

    std::vector<limestone::internal::wal_history::record> records;
    records.reserve(remote_history.size());
    for (const auto& rec : remote_history) {
        if (rec.epoch <= finish_epoch) {
            records.emplace_back(limestone::internal::wal_history::record{
                rec.epoch,
                rec.identity,
                static_cast<std::time_t>(rec.timestamp)
            });
        }
    }

    try {
        limestone::internal::wal_history history(output_dir);
        history.write_records(records);
    } catch (const std::exception& ex) {
        error_message = ex.what();
        return false;
    } catch (...) {
        error_message = "unknown error while writing wal_history";
        return false;
    }

    return true;
}


void wal_sync_client::cleanup_detached_pwals(limestone::api::datastore& ds) {
    try {
        // Obtain compaction catalog via datastore_impl accessor
        auto impl = ds.get_impl();
        compaction_catalog& catalog = impl->get_compaction_catalog();

        std::set<std::string> detached = catalog.get_detached_pwals();
        std::set<compacted_file_info> compacted_files = catalog.get_compacted_files();
        epoch_id_type max_epoch = catalog.get_max_epoch_id();
        blob_id_type max_blob = catalog.get_max_blob_id();

        if (detached.empty()) {
            return;
        }

        boost::system::error_code ec;
        for (auto it = detached.begin(); it != detached.end();) {
            const std::string filename = *it;
            // skip compacted snapshot files
            if (filename == compaction_catalog::get_compacted_filename()
                || filename == compaction_catalog::get_compacted_backup_filename()) {
                ++it;
                continue;
            }
            boost::filesystem::path p = log_dir_ / filename;
            file_ops_->remove(p, ec);
            if (ec) {
                LOG(ERROR) << "failed to remove detached pwal: " << p.string() << ", ec=" << ec.message();
                ++it; // keep name in catalog for future retry
            } else {
                LOG(INFO) << "removed detached pwal: " << p.string();
                it = detached.erase(it);
            }
        }

        try {
            catalog.update_catalog_file(max_epoch, max_blob, compacted_files, detached);
        } catch (std::exception const& ex) {
            LOG(ERROR) << "failed to update compaction catalog after removing detached pwals: " << ex.what();
        } catch (...) {
            LOG(ERROR) << "failed to update compaction catalog after removing detached pwals due to unknown error";
        }
    } catch (std::exception const& ex) {
        LOG(ERROR) << "cleanup_detached_pwals failed: " << ex.what();
    } catch (...) {
        LOG(ERROR) << "cleanup_detached_pwals failed due to unknown error";
    }
}


void wal_sync_client::set_file_operations(file_operations& file_ops) {
    file_ops_ = &file_ops;
}

} // namespace limestone::internal
