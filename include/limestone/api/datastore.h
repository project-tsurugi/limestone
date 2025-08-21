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
#pragma once

#include <memory>
#include <future>
#include <atomic>
#include <chrono>
#include <vector>
#include <set>
#include <mutex>
#include <queue>
#include <map>

#include <boost/filesystem.hpp>

#include <limestone/status.h>
#include <limestone/api/blob_pool.h>
#include <limestone/api/blob_file.h>
#include <limestone/api/backup.h>
#include <limestone/api/backup_detail.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/configuration.h>
#include <limestone/api/file_set_entry.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/tag_repository.h>
#include <limestone/api/restore_progress.h>
#include <limestone/api/rotation_result.h>

namespace limestone::internal {
    class compaction_catalog;
    class blob_file_resolver; 
    class blob_file_garbage_collector;
}
namespace limestone::api {

class datastore_impl;
class backup;
class log_channel;

/**
 * @brief datastore interface to start/stop the services, store log, create snapshot for recover from log files
 * @details this object is not thread-safe except for create_channel().
 */
class datastore {
    friend class log_channel;

    enum class state : std::int64_t {
        not_ready = 0,
        ready = 1,
        shutdown = 2,
    };

public:
    /**
     * @brief create empty object
     * @note this is for test purpose only, must not be used for any purpose other than testing
     */
    datastore() noexcept;

    /**
     * @brief create an object with the given configuration
     * @param conf a reference to a configuration object used in the object construction
     * @exception limestone_exception if an I/O error occurs during construction
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     */
    explicit datastore(configuration const& conf);

    /**
     * @brief destruct the object
     */
    virtual ~datastore() noexcept;

    datastore(datastore const& other) = delete;
    datastore& operator=(datastore const& other) = delete;
    datastore(datastore&& other) noexcept = delete;
    datastore& operator=(datastore&& other) noexcept = delete;

    /**
     * @brief create snapshot from log files stored in location_
     * @details file name of snapshot to be created is snapshot::file_name_ which is stored in location_ / snapshot::subdirectory_name_.
     * If location_ / snapshot::subdirectory_name_ / snapshot::file_name_ is exist, do nothing.
     * @attention this function is not thread-safe.
     * @note overwrite flag is deplicated
     */
    void recover() const noexcept;

    /**
     * @brief restore log files, etc. located at from directory
     * @details log file, etc. stored in from directroy are to be copied to log directory.
     * @param from the location of the log files backuped
     * @attention this function is not thread-safe.
     * @return status indicating whether the process ends successfully or not
     */
    status restore(std::string_view from, bool keep_backup) const noexcept;

    // restore (prusik era)
    status restore(std::string_view from, std::vector<file_set_entry>& entries) noexcept;

    /**
     * @brief returns the status of the restore process that is currently in progress or that has finished immediately before
     * @attention this function is not thread-safe.
     * @return the status of the restore process
     */
    restore_progress restore_status() const noexcept;

    /**
     * @brief transition this object to an operational state
     * @details after this method is called, create_channel() can be invoked.
     * @exception limestone_io_exception Thrown if an I/O error occurs.
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe, and the from directory must not contain any files other than log files.
     */
    void ready();

    /**
     * @brief provides a pointer of the snapshot object
     * @details snapshot used is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return the reference to the object associated with the latest available snapshot
     */
    std::unique_ptr<snapshot> get_snapshot() const;

    /**
     * @brief provides a shared pointer of the snapshot object
     * @details snapshot is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return a shared pointer to the object associated with the latest available snapshot
     */
    std::shared_ptr<snapshot> shared_snapshot() const;

    /**
     * @brief create a log_channel to write logs to a file
     * @details logs are written to separate files created for each channel
     * @param location specifies the directory of the log files
     * @return the reference of the log_channel
     * @attention this function should be called before the ready() is called.
     */
    log_channel& create_channel(const boost::filesystem::path& location);

    /**
     * @brief provide the largest epoch ID
     * @return the largest epoch ID that has been successfully persisted
     * @note designed to make epoch ID monotonic across reboots
     * @attention this function should be called after the ready() is called.
     */
    epoch_id_type last_epoch() const noexcept;

    /**
     * @brief change the current epoch ID
     * @param new epoch id which must be greater than current epoch ID.
     * @exception limestone_io_exception Thrown if an I/O error occurs.
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function should be called after the ready() is called.
     */
    void switch_epoch(epoch_id_type epoch_id);

    /**
     * @brief register a callback on successful persistence
     * @param callback a pointer to the callback function
     * @attention this function should be called before the ready() is called.
     */
    void add_persistent_callback(std::function<void(epoch_id_type)> callback) noexcept;

    /**
     * @brief notify this of the location of available safe snapshots
     * @param write_version specifies the location (write_version) of available safe snapshots,
     * consisting of major and minor versions where major version should be less than or equal to the current epoch ID.
     * @param inclusive specifies the parameter write_version is inclusive or not
     * @attention this function should be called after the ready() is called.
     * @note the actual safe snapshot location can be checked via add_safe_snapshot_callback
     * @note immediately after datastore::ready(), the last_epoch is treated as the maximum write version
     * with last_epoch as the write major version.
     */
    void switch_safe_snapshot(write_version_type write_version, bool inclusive) const noexcept;

    /**
     * @brief register a callback to be called when the safe snapshot location is changed internally
     * @param callback a pointer to the callback function
     * @attention this function should be called before the ready() is called.
     */
    void add_snapshot_callback(std::function<void(write_version_type)> callback) noexcept;

    /**
     * @brief prohibits new persistent sessions from starting thereafter
     * @detail move to the stop preparation state.
     * @return the future of void, which allows get() after the transition to the stop preparation state.
     */
    std::future<void> shutdown() noexcept;

    /**
     * @brief start backup operation
     * @detail a backup object is created, which contains a list of log files.
     * @exception limestone_io_exception Thrown if an I/O error occurs.
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @return a reference to the backup object.
     */
    backup& begin_backup();

    // backup (prusik era)
    /**
     * @brief start backup operation
     * @detail a backup_detail object is created, which contains a list of log entry.
     * @exception limestone_io_exception Thrown if an I/O error occurs.
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @return a reference to the backup_detail object.
     */
    std::unique_ptr<backup_detail> begin_backup(backup_type btype);

    /**
     * @brief provide epoch tag repository
     * @return a reference to the epoch tag repository
     * @note available both before and after ready() call
     */
    tag_repository& epoch_tag_repository() noexcept;

    /**
     * @brief rewinds the state of the data store to the point in time of the specified epoch
     * @detail create a snapshot file for the specified epoch.
     * @attention this function should be called before the ready() is called.
     */
    void recover(const epoch_tag&) const noexcept;

    /**
     * @brief Performs online log file compaction.
     *
     * This function compacts log files that have been rotated and are no longer subject to new entries.
     *
     * @exception limestone_exception Thrown if an I/O error occurs during the compaction process.
     * @attention this function should be called before the ready() is called.
     */
    void compact_with_online();

    /**
     * @brief acquires a new empty BLOB pool.
     * @details This pool is used for temporary registration of BLOBs,
     *      and all BLOBs that are not fully registered will become unavailable when the pool is destroyed.
     * @return the created BLOB pool
     * @see blob_pool::release()
     * @attention the returned BLOB pool must be released by the blob_pool::release() after the usage, or it may cause leaks of BLOB data.
     * @attention Undefined behavior if using pool after destroying this datastore.
     */
    [[nodiscard]] std::unique_ptr<blob_pool> acquire_blob_pool();

    /**
     * @brief returns BLOB file for the BLOB reference.
     * @param reference the target BLOB reference
     * @return the corresponding BLOB file
     * @return unavailable BLOB file if there is no BLOB file for the reference,
     *   that is, the BLOB file has not been registered or has already been removed.
     * @attention the returned BLOB file is only effective
     *    during the transaction that has provided the corresponded BLOB reference.
     */
    [[nodiscard]] blob_file get_blob_file(blob_id_type reference);


    /**
     * @brief change the available boundary version that the entries may be read.
     * @details This version comprises the oldest accessible snapshot, that is,
     *    the datastore may delete anything older than the version included in this snapshot.
     * @details The boundary version must be monotonic, that is,
     *    it must be greater than or equal to the previous boundary version.   
     * @param version the target boundary version
     * @attention this function should be called after the ready() is called.
     * @see switch_safe_snapshot()
     * @note the specified version must be smaller than or equal to the version that was told by the switch_safe_snapshot().
     */
    void switch_available_boundary_version(write_version_type version);


    /**
     * @brief Adds a list of persistent blob IDs to the datastore.
     *
     * This function takes a vector of blob IDs and adds them to the datastore,
     * ensuring that they are stored persistently.
     *
     * NOTE: This method is intended for internal use only. It is used by blob_pool::release
     * to determine whether a given blob ID should be deleted. Once a blob ID is confirmed
     * as not subject to deletion, its entry is removed from the persistent tracking.
     *
     * @param blob_ids A vector containing the blob IDs to be added.
     */
    void add_persistent_blob_ids(const std::vector<blob_id_type>& blob_ids);

    /**
     * @brief Checks and removes persistent blob IDs from the given list.
     *
     * This function takes a vector of blob IDs and checks for their persistence.
     * It is used by blob_pool::release to determine whether a given blob ID should be deleted.
     * Once a blob ID is confirmed as not subject to deletion, its entry is removed from the persistent tracking.
     *
     * NOTE: This method is intended for internal use only.
     *
     * @param blob_ids A vector of blob IDs to be checked and potentially removed.
     * @return A vector of blob IDs that were persistent and have been removed.
     */
    std::vector<blob_id_type> check_and_remove_persistent_blob_ids(const std::vector<blob_id_type>& blob_ids);

    /**
     * @brief Retrieves a pointer to the underlying datastore implementation.
     *
     * This method returns a non-owning pointer to the internal
     * datastore_impl instance.
     *
     *
     * NOTE: This method is intended for internal use only.
     * 
     * @return A pointer to the datastore implementation.
     */
    datastore_impl* get_impl() noexcept { return impl_.get(); }

    /**
     * @brief Writes the specified epoch id to the epoch file and notifies replicas if needed.
     *
     * This function writes the specified epoch id into the epoch file.
     * If there are any replicas that require the epoch id to be updated, they will be notified.
     *
     * NOTE: This method is intended for internal use only.
     *
     * @param epoch_id The epoch id to be written and, if necessary, propagated to replicas.
     */
    virtual void persist_and_propagate_epoch_id(epoch_id_type epoch_id);

protected:  // for tests
    auto& log_channels_for_tests() const noexcept { return log_channels_; }
    auto epoch_id_informed_for_tests() const noexcept { return epoch_id_informed_.load(); }
    auto epoch_id_to_be_recorded_for_tests() const noexcept { return epoch_id_to_be_recorded_.load(); }
    auto epoch_id_record_finished_for_tests() const noexcept { return epoch_id_record_finished_.load(); }
    auto epoch_id_switched_for_tests() const noexcept { return epoch_id_switched_.load(); }
    auto next_blob_id_for_tests() const noexcept { return next_blob_id_.load(); }
    auto& files_for_tests() const noexcept { return files_; }
    void rotate_epoch_file_for_tests() { rotate_epoch_file(); }
    void set_next_blob_id_for_tests(blob_id_type next_blob_id) noexcept { next_blob_id_.store(next_blob_id); }
    std::set<blob_id_type> get_persistent_blob_ids_for_tests() noexcept {
        std::lock_guard<std::mutex> lock(persistent_blob_ids_mutex_);
        return persistent_blob_ids_;
    }
    write_version_type get_available_boundary_version_for_tests() const noexcept { return available_boundary_version_; }
    void wait_for_blob_file_garbage_collector_for_tests() const noexcept;


    // These virtual methods are hooks for testing thread synchronization.
    // They allow derived classes to inject custom behavior or notifications
    // at specific wait points during the execution of the datastore class.
    // The default implementation does nothing, ensuring no impact on production code.
    virtual void on_rotate_log_files() noexcept {}
    virtual void on_begin_session_current_epoch_id_store() noexcept {}
    virtual void on_end_session_finished_epoch_id_store() noexcept {}
    virtual void on_end_session_current_epoch_id_store() noexcept {}
    virtual void on_switch_epoch_epoch_id_switched_store() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_switched_load() noexcept {}
    virtual void on_update_min_epoch_id_current_epoch_id_load() noexcept {}
    virtual void on_update_min_epoch_id_finished_epoch_id_load() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_to_be_recorded_load() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_to_be_recorded_cas() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_record_finished_load() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_informed_load_1() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_informed_cas() noexcept {}
    virtual void on_update_min_epoch_id_epoch_id_informed_load_2() noexcept {}

    /**
     * @brief Sets the callback function for writing epoch to a file.
     * @details
     * This method allows you to override the default behavior for writing epoch 
     * information by providing a custom callback. The callback can be a free 
     * function, a lambda, or a member function bound to an object.
     * 
     * Example:
     * @code
     * class CustomHandler {
     * public:
     *     void custom_epoch_writer(epoch_id_type epoch) {
     *         // Custom logic
     *     }
     * };
     * 
     * datastore ds;
     * CustomHandler handler;
     * ds.set_write_epoch_callback([&handler](epoch_id_type epoch) {
     *     handler.custom_epoch_writer(epoch);
     * });
     * @endcode
     * 
     * @param callback The new callback function to use for writing epoch.
     */
    void set_write_epoch_callback(std::function<void(epoch_id_type)> callback) {
        write_epoch_callback_ = std::move(callback);
    }

private:
    void persist_epoch_id(epoch_id_type epoch_id);

    std::function<void(epoch_id_type)> write_epoch_callback_{
        [this](epoch_id_type epoch) { this->persist_and_propagate_epoch_id(epoch); }
    };

    std::vector<std::unique_ptr<log_channel>> log_channels_;

    boost::filesystem::path location_{};

    std::atomic_uint64_t epoch_id_switched_{};

    std::atomic_uint64_t epoch_id_informed_{};

    std::atomic_uint64_t epoch_id_to_be_recorded_{};
    std::atomic_uint64_t epoch_id_record_finished_{};

    std::unique_ptr<backup> backup_{};

    std::function<void(epoch_id_type)> persistent_callback_;

    std::function<void(write_version_type)> snapshot_callback_;

    boost::filesystem::path epoch_file_path_{};

    boost::filesystem::path tmp_epoch_file_path_{};

    tag_repository tag_repository_{};

    std::atomic_uint64_t log_channel_id_{};

    std::future<void> online_compaction_worker_future_;

    std::mutex mtx_online_compaction_worker_{};

    std::condition_variable cv_online_compaction_worker_{};

    std::atomic<bool> stop_online_compaction_worker_{false};

    std::unique_ptr<limestone::internal::compaction_catalog> compaction_catalog_;

    void online_compaction_worker();

    void stop_online_compaction_worker();

    // used for backup
    //   (old) full backup :   target is entire <files_>
    //   (new/prusik) backup : target is rotated files, i.e. <files_> minus active log files
    std::set<boost::filesystem::path> files_{};

    std::mutex mtx_channel_{};

    std::mutex mtx_files_{};

    int recover_max_parallelism_{};

    std::mutex mtx_epoch_file_{};

    std::mutex mtx_epoch_persistent_callback_{};

    state state_{};

    void add_file(const boost::filesystem::path& file) noexcept;

    // opposite of add_file
    void subtract_file(const boost::filesystem::path& file);

    std::set<boost::filesystem::path> get_files();

    epoch_id_type search_max_durable_epock_id() noexcept;

    void update_min_epoch_id(bool from_switch_epoch = false);
    
    void check_after_ready(std::string_view func) const noexcept;

    void check_before_ready(std::string_view func) const noexcept;

    /**
     * @brief Creates a snapshot of the current state and retrieves the maximum blob ID.
     *
     * This function creates a snapshot of the current state of the datastore and returns the maximum blob ID present in the datastore.
     * It is useful for ensuring data consistency and for retrieving the highest blob ID for further processing or reference.
     */
    blob_id_type create_snapshot_and_get_max_blob_id();


    /**
     * @brief requests the data store to rotate log files
     */
    rotation_result rotate_log_files();

    // Mutex to protect rotate_log_files from concurrent access
    std::mutex rotate_mutex;

    // Mutex and condition variable for synchronizing epoch_id_informed_ updates.
    std::mutex informed_mutex;
    std::condition_variable cv_epoch_informed;

    /**
     * @brief rotate epoch file
     */
    void rotate_epoch_file();

    int64_t current_unix_epoch_in_millis();

    std::map<storage_id_type, write_version_type> clear_storage;  

    // File descriptor for file lock (flock) on the manifest file
    int fd_for_flock_{-1};

    
    int epoch_write_counter = 0;

    std::unique_ptr<limestone::internal::blob_file_resolver> blob_file_resolver_;

    std::atomic<std::uint64_t> next_blob_id_{0};

    std::set<blob_id_type> persistent_blob_ids_;

    std::mutex persistent_blob_ids_mutex_;

    std::unique_ptr<limestone::internal::blob_file_garbage_collector> blob_file_garbage_collector_;

    // Boundary version for safe snapshots
    write_version_type available_boundary_version_; 

    // Mutex to protect boundary version updates
    mutable std::mutex boundary_mutex_;

    // Use Pimpl idiom to hide implementation details, improve encapsulation,
    // and minimize compilation dependencies.
    std::unique_ptr<datastore_impl> impl_;
};

} // namespace limestone::api
