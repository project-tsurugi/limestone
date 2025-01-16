/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <vector>
#include <optional> 

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/storage_id_type.h>


namespace limestone::internal {
    class cursor_impl;  
}

namespace limestone::api {

class log_entry;
class snapshot;


/**
 * @brief a cursor to scan entries on the snapshot
 */
class cursor {
public:
    /**
     * @brief destruct the object
     */
    ~cursor() noexcept;

    cursor(cursor const& other) = delete;
    cursor& operator=(cursor const& other) = delete;
    cursor(cursor&& other) noexcept = delete;
    cursor& operator=(cursor&& other) noexcept = delete;

    /**
     * @brief change the current cursor to point to the next entry
     * @attention this function is not thread-safe.
     * @exception limestone_exception if an error occurs while reading the log entry
     * @note Currently, this function does not throw an exception but logs the error and aborts the process.
     *       However, throwing an exception is the intended behavior, and this will be restored in future versions.
     *       Therefore, callers of this API must handle the exception properly as per the original design.
     * @return true if the next entry exists, false otherwise
     */
    bool next();

    /**
     * @brief returns the storage ID of the entry at the current cursor position
     * @return the storage ID of the current entry
     */
    [[nodiscard]] storage_id_type storage() const noexcept;

    /**
     * @brief returns the key byte string of the entry at the current cursor position
     * @param buf a reference to a byte string in which the key is stored
     */
    void key(std::string& buf) const noexcept;

    /**
     * @brief returns the value byte string of the entry at the current cursor position
     * @param buf a reference to a byte string in which the value is stored
     */
    void value(std::string& buf) const noexcept;

private:
    std::unique_ptr<internal::cursor_impl> pimpl;

    explicit cursor(const boost::filesystem::path& snapshot_file);
    explicit cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file);

    friend class internal::cursor_impl;
};

} // namespace limestone::api
