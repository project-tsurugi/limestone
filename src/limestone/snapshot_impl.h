/*
 * Copyright 2022-2026 Project Tsurugi.
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

#include <limestone/api/cursor.h>
#include <limestone/api/write_version_type.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "locator/locator.h"

namespace limestone::internal {

using limestone::api::cursor;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

class snapshot_impl {
public:
    /**
     * @param location the data location
     * @param clear_storage the write version per storage below which entries are skipped
     * @param compacted_file_name the name of the current compacted WAL file recorded in the catalog, or nullopt
     * @param region_records the number of locator records in one region of get_partitioned_cursors() (R)
     */
    explicit snapshot_impl(boost::filesystem::path location, std::map<storage_id_type, write_version_type> clear_storage,
                           std::optional<std::string> compacted_file_name, std::size_t region_records) noexcept;

    /**
     * @brief Returns the cursors for reading the snapshot shared out among n cursors.
     *
     * Reads the locators of the snapshot file and the compacted WAL file (if the catalog has one), creates one region
     * allocator, and returns n cursors that share it. Each cursor opens its files here (they remain readable after
     * they are removed). The regions are taken in next() of the cursors.
     * @param n the number of cursors
     * @return n cursors
     * @throws std::invalid_argument if n is 0
     * @throws limestone_exception on the second call, or if a locator file is invalid
     * @throws limestone_io_exception if a file cannot be opened or read
     */
    std::vector<std::unique_ptr<limestone::api::cursor>> get_partitioned_cursors(std::size_t n);
    [[nodiscard]] std::unique_ptr<cursor> get_cursor() const;

private:
    /**
     * @brief Reads the locator file of the file.
     * @throws limestone_exception if the locator file is invalid
     * @throws limestone_io_exception if the size of the file cannot be obtained, or the locator file cannot be read
     */
    [[nodiscard]] static locator read_locator(const boost::filesystem::path& locatee);

    boost::filesystem::path location_;
    std::map<storage_id_type, write_version_type> clear_storage;
    std::optional<std::string> compacted_file_name_;  ///< name of the current compacted file recorded in the catalog; std::nullopt if there is none
    std::size_t region_records_;
    std::atomic<bool> partitioned_called_{false};
};

} // namespace limestone::internal