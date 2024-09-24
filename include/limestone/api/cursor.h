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

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/large_object_view.h>

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
     * @return true if the next entry exists, false otherwise
     */
    bool next();

    /**
     * @brief returns the storage ID of the entry at the current cursor position
     * @return the storage ID of the current entry
     */
    storage_id_type storage() const noexcept;

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

    /**
     * @brief returns a list of large objects associated with the entry at the current cursor position
     * @return a list of large objects associated with the current entry
     */
    std::vector<large_object_view>& large_objects() noexcept;

private:
    boost::filesystem::ifstream istrm_{};
    std::unique_ptr<log_entry> log_entry_;
    std::vector<large_object_view> large_objects_{};

    explicit cursor(const boost::filesystem::path& file) noexcept;
 
    friend class snapshot;
};

} // namespace limestone::api
