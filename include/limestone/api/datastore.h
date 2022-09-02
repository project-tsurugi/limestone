/*
 * Copyright 2022-2022 tsurugi project.
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
#include <vector>
#include <set>
#include <mutex>
#include <atomic>

#include <boost/filesystem/path.hpp>

#include <limestone/status.h>
#include <limestone/api/backup.h>
#include <limestone/api/log_channel.h>
#include <limestone/api/configuration.h>
#include <limestone/api/snapshot.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/tag_repository.h>
#include <limestone/api/restore_progress.h>

namespace limestone::api {

/**
 * @brief datastore interface to start/stop the services, store log, create snapshot for recover from log files
 * @details this object is not thread-safe except for create_channel().
 */
class datastore {
    friend class log_channel;

    /**
     * @brief name of a file to record durable epoch
     */
    static constexpr const std::string_view epoch_file_name = "epoch";  // NOLINT

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
     */
    explicit datastore(configuration const& conf) noexcept;

    /**
     * @brief destruct the object
     */
    ~datastore() noexcept;

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

    /**
     * @brief returns the status of the restore process that is currently in progress or that has finished immediately before
     * @attention this function is not thread-safe.
     * @return the status of the restore process
     */
    restore_progress restore_status() const noexcept;

    /**
     * @brief transition this object to an operational state
     * @details after this method is called, create_channel() can be invoked.
     * @attention this function is not thread-safe, and the from directory must not contain any files other than log files.
     */
    void ready() noexcept;

    /**
     * @brief provides a pointer of the snapshot object
     * @details snapshot used is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return the reference to the object associated with the latest available snapshot
     */
    std::unique_ptr<snapshot> get_snapshot() const noexcept;

    /**
     * @brief provides a shared pointer of the snapshot object
     * @details snapshot is location_ / snapshot::subdirectory_name_ / snapshot::file_name_
     * @return a shared pointer to the object associated with the latest available snapshot
     */
    std::shared_ptr<snapshot> shared_snapshot() const noexcept;

    /**
     * @brief create a log_channel to write logs to a file
     * @details logs are written to separate files created for each channel
     * @param location specifies the directory of the log files
     * @return the reference of the log_channel
     * @attention this function should be called before the ready() is called.
     */
    log_channel& create_channel(const boost::filesystem::path& location) noexcept;

    /**
     * @brief provide the largest epoch ID
     * @return the largest epoch ID that has been successfully persisted
     * @note designed to make epoch ID monotonic across reboots
     */
    epoch_id_type last_epoch() const noexcept;

    /**
     * @brief change the current epoch ID
     * @param new epoch id which must be greater than current epoch ID.
     * @attention this function should be called after the ready() is called.
     */
    void switch_epoch(epoch_id_type epoch_id) noexcept;

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
     * @return a reference to the backup object.
     */
    backup& begin_backup() noexcept;

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

protected:
    std::vector<std::unique_ptr<log_channel>> log_channels_;  //  NOLINT (place in protectes region for tests)
    
private:
    boost::filesystem::path location_{};

    std::atomic_uint64_t epoch_id_switched_{};

    std::atomic_uint64_t epoch_id_informed_{};

    std::unique_ptr<backup> backup_{};

    std::function<void(epoch_id_type)> persistent_callback_;

    std::function<void(write_version_type)> snapshot_callback_;

    boost::filesystem::path epoch_file_path_{};

    tag_repository tag_repository_{};

    std::atomic_uint64_t log_channel_id_{};

    std::set<boost::filesystem::path> files_{};

    std::mutex mtx_channel_{};

    std::mutex mtx_files_{};

    state state_{};

    void add_file(const boost::filesystem::path& file) noexcept;

    bool update_min_epoch_id() noexcept;
    
    void check_after_ready(std::string_view func) const noexcept;

    void check_before_ready(std::string_view func) const noexcept;

    /**
     * @brief create snapshot from log files stored in the location directory
     * @details file name of snapshot to be created is snapshot::file_name_ which is stored in location_ / snapshot::subdirectory_name_.
     * @param from the location of log files
     * @attention this function is not thread-safe.
     */
    void create_snapshot() noexcept;
};

} // namespace limestone::api
