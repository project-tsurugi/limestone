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

#include <cstdio>
#include <string>
#include <string_view>
#include <cstdint>
#include <atomic>
#include <set>
#include <memory>
#include <condition_variable>

#include <boost/filesystem.hpp>

#include <limestone/status.h>
#include <limestone/api/blob_id_type.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

namespace limestone::api {

class datastore;
class rotation_result;
class log_channel_impl;

/**
 * @brief log_channel interface to output logs
 * @details this object is not thread-safe, assuming each thread uses its own log_channel
 */
class log_channel {

public:
   /**
     * @brief join a persistence session for the current epoch in this channel
     * @attention this function is not thread-safe.
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @note the current epoch is the last epoch specified by datastore::switch_epoch()
     * @note datastore::switch_epoch() and this function can be called simultaneously.
     * If these functions are invoked at the same time, the result will be as if one of them was called first, 
     * but it is indeterminate which one will take precedence.
     */
    void begin_session();

    /**
     * @brief notifies the completion of an operation in this channel for the current persistent session the channel is participating in
     * @attention this function is not thread-safe.
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @note when all channels that have participated in the current persistent session call end_session() and the current epoch is
     * greater than the session's epoch, the persistent session itself is complete
     */
    void end_session();

    /**
     * @brief terminate the current persistent session in which this channel is participating with an error
     * @attention this function is not thread-safe.
     */
    void abort_session(status status_code, const std::string& message) noexcept;

    /**
     * @brief adds an entry to the current persistent session
     * @param storage_id the storage ID of the entry to be added
     * @param key the key byte string for the entry to be added
     * @param value the value byte string for the entry to be added
     * @param write_version (optional) the write version of the entry to be added. If omitted, the default value is used
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     */
    void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version);

    /**
     * @brief adds an entry to the current persistent session
     * @param storage_id the storage ID of the entry to be added
     * @param key the key byte string for the entry to be added
     * @param value the value byte string for the entry to be added
     * @param write_version (optional) the write version of the entry to be added. If omitted, the default value is used
     * @param large_objects (optional) the list of large objects associated with the entry to be added
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     */
    void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, const std::vector<blob_id_type>& large_objects);

    /**
     * @brief add an entry indicating the deletion of entries
     * @param storage_id the storage ID of the entry to be deleted
     * @param key the key byte string for the entry to be deleted
     * @param write_version the write version of the entry to be removed
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the entries to be deleted are treated as if they do not exist in a recover() operation from a log stored in the current persistent session
     */
    void remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version);

    /**
     * @brief add an entry indicating the addition of the specified storage
     * @param storage_id the storage ID of the entry to be added
     * @param write_version the write version of the entry to be added
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     * @impl this operation may be ignored.
     */
    void add_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief add an entry indicating the deletion of the specified storage and all entries for that storage
     * @param storage_id the storage ID of the entry to be removed
     * @param write_version the write version of the entry to be removed
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the target entries are treated as if they do not exist in the recover() operation from the log stored in the current persistent session.
     */
    void remove_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief add an entry indicating the deletion of all entries contained in the specified storage
     * @param storage_id the storage ID of the entry to be removed
     * @param write_version the write version of the entry to be removed
     * @exception limestone_exception if I/O error occurs
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the target entries are treated as if they do not exist in the recover() operation from the log stored in the current persistent session.
     */
    void truncate_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief this is for test purpose only, must not be used for any purpose other than testing
     */
    [[nodiscard]] boost::filesystem::path file_path() const noexcept;

    /**
     * @brief this is for test purpose only, must not be used for any purpose other than testing
     */
    [[nodiscard]] auto current_epoch_id() const noexcept { return current_epoch_id_.load(); }

    /**
     * @brief this is for test purpose only, must not be used for any purpose other than testing
     */
    [[nodiscard]] auto finished_epoch_id() const noexcept { return finished_epoch_id_.load(); }


    [[nodiscard]] log_channel_impl* get_impl() const noexcept;
private:
    void finalize_session_file();

    datastore& envelope_;

    boost::filesystem::path location_;

    boost::filesystem::path file_;

    std::size_t id_{};

    FILE* strm_{};

    bool registered_{};

    std::atomic_uint64_t current_epoch_id_{UINT64_MAX};

    std::atomic_uint64_t finished_epoch_id_{0};

    std::string do_rotate_file(epoch_id_type epoch = 0);

    std::unique_ptr<log_channel_impl> impl_;


protected: // Protected to allow testing with derived classes
    log_channel(boost::filesystem::path location, std::size_t id, datastore& envelope) noexcept;
 
    friend class datastore;
    friend class datastore_impl;
    friend class rotation_task;
};

} // namespace limestone::api
