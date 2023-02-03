/*
 * Copyright 2022-2023 tsurugi project.
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

#include <string>
#include <string_view>
#include <cstdint>
#include <atomic>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/status.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/large_object_input.h>

namespace limestone::api {

/**
 * @brief log_channel interface to output logs
 * @details this object is not thread-safe, assuming each thread uses its own log_channel
 */
class log_channel {
public:
    /**
     * @brief join a persistence session for the current epoch in this channel
     * @attention this function is not thread-safe.
     * @note the current epoch is the last epoch specified by datastore::switch_epoch()
     */
    virtual void begin_session() noexcept = 0;

    /**
     * @brief notifies the completion of an operation in this channel for the current persistent session the channel is participating in
     * @attention this function is not thread-safe.
     * @note when all channels that have participated in the current persistent session call end_session() and the current epoch is
     * greater than the session's epoch, the persistent session itself is complete
     */
    virtual void end_session() noexcept = 0;

    /**
     * @brief terminate the current persistent session in which this channel is participating with an error
     * @attention this function is not thread-safe.
     */
    virtual void abort_session(status status_code, const std::string& message) noexcept = 0;

    /**
     * @brief adds an entry to the current persistent session
     * @param storage_id the storage ID of the entry to be added
     * @param key the key byte string for the entry to be added
     * @param value the value byte string for the entry to be added
     * @param write_version (optional) the write version of the entry to be added. If omitted, the default value is used
     * @attention this function is not thread-safe.
     */
    virtual void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) noexcept = 0;

    /**
     * @brief adds an entry to the current persistent session
     * @param storage_id the storage ID of the entry to be added
     * @param key the key byte string for the entry to be added
     * @param value the value byte string for the entry to be added
     * @param write_version (optional) the write version of the entry to be added. If omitted, the default value is used
     * @param large_objects (optional) the list of large objects associated with the entry to be added
     * @attention this function is not thread-safe.
     */
    virtual void add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, const std::vector<large_object_input>& large_objects) noexcept = 0;

    /**
     * @brief add an entry indicating the deletion of entries
     * @param storage_id the storage ID of the entry to be deleted
     * @param key the key byte string for the entry to be deleted
     * @param write_version the write version of the entry to be removed
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the entries to be deleted are treated as if they do not exist in a recover() operation from a log stored in the current persistent session
     */
    virtual void remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version) noexcept = 0;

    /**
     * @brief add an entry indicating the addition of the specified storage
     * @param storage_id the storage ID of the entry to be added
     * @param write_version the write version of the entry to be added
     * @attention this function is not thread-safe.
     * @impl this operation may be ignored.
     */
    virtual void add_storage(storage_id_type storage_id, write_version_type write_version) noexcept = 0;

    /**
     * @brief add an entry indicating the deletion of the specified storage and all entries for that storage
     * @param storage_id the storage ID of the entry to be removed
     * @param write_version the write version of the entry to be removed
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the target entries are treated as if they do not exist in the recover() operation from the log stored in the current persistent session.
     */
    virtual void remove_storage(storage_id_type storage_id, write_version_type write_version) noexcept = 0;

    /**
     * @brief add an entry indicating the deletion of all entries contained in the specified storage
     * @param storage_id the storage ID of the entry to be removed
     * @param write_version the write version of the entry to be removed
     * @attention this function is not thread-safe.
     * @note no deletion operation is performed on the entry that has been added to the current persistent session, instead,
     * the target entries are treated as if they do not exist in the recover() operation from the log stored in the current persistent session.
     */
    virtual void truncate_storage(storage_id_type storage_id, write_version_type write_version) noexcept = 0;

    /**
     * @brief this is for test purpose only, must not be used for any purpose other than testing
     */
    virtual boost::filesystem::path file_path() const noexcept = 0;
};

} // namespace limestone::api
