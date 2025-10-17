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

#pragma once

#include <chrono>
#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <utility>
#include <functional>
#include <string>
#include <vector>
#include <optional>
#include <exception>
#include <boost/filesystem.hpp>

#include <grpc/client/backup_client.h>
#include <grpc/client/wal_history_client.h>
#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <limestone/api/epoch_id_type.h>
#include <wal_sync/backup_object_type.h>

#include "file_operations.h"

namespace limestone::internal {

using limestone::api::epoch_id_type; 
using limestone::internal::backup_object_type;

using unix_timestamp_seconds = std::int64_t;

class rotation_aware_datastore : public limestone::api::datastore {
public:
    explicit rotation_aware_datastore(limestone::api::configuration const& conf);

    void set_rotation_handler(std::function<void()> handler);
    void trigger_rotation_handler_for_tests();
    virtual void perform_compaction();
    virtual void perform_switch_epoch(epoch_id_type value);

protected:
    void on_rotate_log_files() noexcept override;

private:
    std::function<void()> rotation_handler_{};
};

struct branch_epoch {
    epoch_id_type epoch;
    std::uint64_t identity;
    unix_timestamp_seconds timestamp;
};

struct backup_object {
    std::string id;
    backup_object_type type;
    std::string path;
};

/**
 * @brief Result of begin_backup operation.
 */
struct begin_backup_result {
    std::string session_token;
    std::chrono::system_clock::time_point expire_at;
    std::vector<backup_object> objects;
};

struct remote_backup_result {
    bool success = false;
    std::string error_message;
    std::vector<std::string> incomplete_object_ids;
};

/**
 * @brief Replica WAL sync client 
 */
class wal_sync_client {

public:
    /**
     * @brief Construct wal_sync_client with log directory path and gRPC channel
     * @param log_dir log directory path
     * @param channel gRPC channel to use for wal_history_client
     */
    wal_sync_client(boost::filesystem::path log_dir, std::shared_ptr<::grpc::Channel> const& channel) noexcept;

    wal_sync_client(wal_sync_client const&) = delete;
    wal_sync_client& operator=(wal_sync_client const&) = delete;
    wal_sync_client(wal_sync_client&&) = delete;
    wal_sync_client& operator=(wal_sync_client&&) = delete;

    /**
     * @brief Destructor.
     */
    virtual ~wal_sync_client();

    /**
     * @brief Get the epoch value from the remote backup service.
     * @return remote node's durable epoch value; std::nullopt on failure
     */
    std::optional<epoch_id_type> get_remote_epoch();

    /**
     * @brief Get the epoch value of the local node.
     * @return local node's durable epoch value
     */
    epoch_id_type get_local_epoch();

    /**
     * @brief Get WAL compatibility info from the remote backup service.
     * @return WAL history/compatibility info; std::nullopt on failure
     */
    std::optional<std::vector<branch_epoch>> get_remote_wal_compatibility();

    /**
     * @brief Get WAL compatibility info of the local node.
     * @return WAL history/compatibility info
     */
    std::vector<branch_epoch> get_local_wal_compatibility();

    /**
     * @brief Compare local and remote WAL compatibility info.
     * @param local local WAL history
     * @param remote remote WAL history
     * @return true if compatible, false otherwise
     */
    bool check_wal_compatibility(
        std::vector<branch_epoch> const& local,
        std::vector<branch_epoch> const& remote
    );

    /**
     * @brief Start backup session and get list of backup objects.
     * @param begin_epoch start epoch (inclusive)
     * @param end_epoch end epoch (exclusive, 0 for latest)
     * @return result containing session token, expiration, and objects; std::nullopt on failure
     */
    std::optional<begin_backup_result> begin_backup(
        std::uint64_t begin_epoch,
        std::uint64_t end_epoch
    );

    /**
     * @brief Execute remote backup end-to-end sequence.
     *
     * This method performs begin_backup(), copy_backup_objects(), and end_backup()
     * as a single operation. While copying, it periodically calls keepalive_session()
     * on a background thread to keep the remote session alive. The interval is defined by
     * limestone::grpc::service::keepalive_interval_ms.
     *
     * @param begin_epoch start epoch (inclusive) passed to begin_backup()
     * @param end_epoch end epoch (exclusive, 0 for latest) passed to begin_backup()
     * @param output_dir directory where retrieved objects are written
     * @return remote_backup_result with success flag, error message, and incomplete object IDs
     */
    remote_backup_result execute_remote_backup(
        std::uint64_t begin_epoch,
        std::uint64_t end_epoch,
        boost::filesystem::path const& output_dir
    );

    /**
     * @brief Restore backup objects from the remote node.
     * @param begin_epoch start epoch (inclusive) passed to execute_remote_backup()
     * @param end_epoch end epoch (exclusive, 0 for latest) passed to execute_remote_backup()
     * @param output_dir directory where restore targets are placed
     * @return true if restore succeeded
     */
    bool restore(
        std::uint64_t begin_epoch,
        std::uint64_t end_epoch,
        boost::filesystem::path const& output_dir
    );

    /**
     * @brief Initialize the client and validate or initialize the log directory and manifest.
     *
     * - If log_dir_ does not exist:
     *     - allow_initialize=true: create_directory is called and manifest is initialized.
     *     - allow_initialize=false: returns error.
     * - If log_dir_ exists and is empty:
     *     - allow_initialize=true: manifest is initialized.
     *     - allow_initialize=false: returns error.
     * - If log_dir_ exists and is not empty:
     *     - manifest version etc. are validated.
     *
     * @param error_message Set to error reason on failure.
     * @param allow_initialize Whether to allow initialization if directory does not exist or is empty.
     * @return true: success, false: error (see error_message)
     */
    bool init(std::string& error_message, bool allow_initialize);

    /**
     * @brief Set custom file_operations implementation (for testing)
     * @param file_ops file_operations implementation to use
     */
    void set_file_operations(file_operations& file_ops);

protected:
    /**
     * @brief Merge/compact WAL files if needed after incremental backup.
     * @return true if compaction succeeded
     */
    bool compact_wal();

    /**
     * @brief Request copy of backup objects from remote.
     *
     * This function assumes that the `objects` parameter is the list obtained from begin_backup().
     * When using any other source, the caller must perform necessary validation beforehand.
     *
     * @param session_token session token
     * @param objects list of objects to copy (begin_backup() result is assumed)
     * @param output_dir directory where retrieved objects are written
     * @return remote_backup_result containing success flag, error message, and incomplete object IDs
     */
    remote_backup_result copy_backup_objects(
        std::string const& session_token,
        std::vector<backup_object> const& objects,
        boost::filesystem::path const& output_dir
    );

    /**
     * @brief Create rotation-aware datastore for compaction.
     * @return rotation-aware datastore instance
     */
    virtual std::unique_ptr<rotation_aware_datastore> create_rotation_aware_datastore();

    /**
     * @brief Prepare datastore for compaction.
     * @param datastore datastore instance
     * @param rotation_triggered flag to be set when rotation happens
     * @param rotation_cv condition variable used for rotation synchronization
     * @param rotation_mutex mutex guarding rotation states
     * @return pair of epoch and success flag; returns {0, false} on failure
     */
    virtual std::pair<epoch_id_type, bool> prepare_for_compaction(
        rotation_aware_datastore& datastore,
        std::atomic<bool>& rotation_triggered,
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex
    );

    /**
     * @brief Invoke datastore::ready() hook (overridable for testing).
     * @param datastore datastore instance to prepare
     */
    virtual void ready_datastore(rotation_aware_datastore& datastore);

    /**
     * @brief Retrieve last durable epoch (overridable for testing).
     * @param datastore datastore instance
     * @return last durable epoch
     */
    [[nodiscard]] virtual epoch_id_type query_last_epoch(rotation_aware_datastore const& datastore) const;

    /**
     * @brief Execute compaction workflow with rotation synchronization.
     * @param datastore datastore instance
     * @param current_epoch current epoch value
     * @param rotation_triggered flag indicating rotation request
     * @param rotation_cv condition variable for rotation
     * @param rotation_mutex mutex guarding rotation state
     * @param compaction_error receives compaction error if thrown
     * @return true on success
     */
    virtual bool run_compaction_with_rotation(
        rotation_aware_datastore& datastore,
        epoch_id_type current_epoch,
        std::atomic<bool>& rotation_triggered,
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex,
        std::exception_ptr& compaction_error
    );


    /**
     * @brief Extend the session expiration.
     * @param session_token session token
     * @return true if extension succeeded
     * @note Declared virtual so tests can override keepalive behavior.
     */
    virtual bool keepalive_session(std::string const& session_token);

    /**
     * @brief End the backup session.
     * @param session_token session token
     * @return true if session ended successfully
     */
    bool end_backup(std::string const& session_token);

private:
    bool wait_for_rotation_or_completion(
        std::atomic<bool>& rotation_triggered,
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex,
        bool& compaction_done,
        std::exception_ptr& compaction_error
    );

    std::thread launch_compaction_thread(
        rotation_aware_datastore& datastore,
        std::exception_ptr& compaction_error,
        std::atomic<bool>& rotation_triggered,
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex,
        bool& compaction_done
    );

    bool handle_rotation_after_trigger(
        rotation_aware_datastore& datastore,
        epoch_id_type current_epoch,
        std::atomic<bool>& rotation_triggered,
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex,
        bool& compaction_done,
        std::exception_ptr& compaction_error
    );

    bool wait_for_compaction_completion(
        std::condition_variable& rotation_cv,
        std::mutex& rotation_mutex,
        bool& compaction_done,
        std::exception_ptr& compaction_error
    );

    boost::filesystem::path log_dir_;
    real_file_operations real_file_ops_;
    file_operations* file_ops_;
    int lock_fd_ = -1;
    std::shared_ptr<limestone::grpc::client::wal_history_client> history_client_;
    std::shared_ptr<limestone::grpc::client::backup_client> backup_client_;
};

} // namespace limestone::internal
