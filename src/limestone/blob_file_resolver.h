/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <boost/filesystem.hpp>
#include <sstream>
#include <iomanip>
#include <vector>

#include <limestone/api/blob_file.h>
#include <limestone/api/blob_id_type.h>

namespace limestone::internal {

using limestone::api::blob_id_type;
using limestone::api::blob_file;

/**
 * @brief Resolves file paths for given BLOB IDs with precomputed directory caching.
 *
 * This class encapsulates the logic for:
 *   - Resolving the file path for a given blob_id.
 *   - Checking if a given path represents a valid blob_file.
 *   - Extracting the blob_id from a blob_file's path.
 *   - Providing the root directory from which BLOB files are stored.
 *
 * BLOB files are assumed to be stored under <base_directory>/blob/ and distributed
 * among several subdirectories.
 */
class blob_file_resolver {
public:
    /**
     * @brief Constructs a blob_file_resolver with the given base directory.
     * 
     * The BLOB files are assumed to be stored under `<base_directory>/blob/`.
     * 
     * @param base_directory The base directory for storing BLOB files.
     */
    explicit blob_file_resolver(boost::filesystem::path base_directory) noexcept
        : blob_directory_(std::move(base_directory) / "blob"),
          hash_function_([](blob_id_type id) { return id; })
    {
        precompute_directory_cache();
    }

    /**
     * @brief Resolves the file path for the given BLOB ID.
     * 
     * @param blob_id The ID of the BLOB.
     * @return The resolved file path.
     */
    [[nodiscard]] boost::filesystem::path resolve_path(blob_id_type blob_id) const noexcept {
        std::size_t directory_index = hash_function_(blob_id) % directory_count_;

        // Retrieve precomputed directory path
        const boost::filesystem::path& subdirectory = directory_cache_[directory_index];

        // Generate the file name
        std::ostringstream file_name;
        file_name << std::hex << std::setw(16) << std::setfill('0') << blob_id << ".blob";

        return subdirectory / file_name.str();
    }

    /**
     * @brief Checks whether the file at the specified path conforms to the expected blob_file format.
     *
     * This function verifies that the file name is formatted as 16 hexadecimal digits followed by the ".blob" extension.
     *
     * @param path The file path to check.
     * @return true if the file is a valid blob_file, false otherwise.
     */
    [[nodiscard]] bool is_blob_file(const boost::filesystem::path& path) const noexcept {
        std::string filename = path.filename().string();
        if (filename.size() != 16 + 5) { // 16 hex digits + ".blob"
            return false;
        }
        if (filename.substr(16) != ".blob") {
            return false;
        }
        for (size_t i = 0; i < 16; ++i) {
            char c = filename[i];
            if (!((c >= '0' && c <= '9') ||
                  (c >= 'A' && c <= 'F') ||
                  (c >= 'a' && c <= 'f'))) {
                return false; // NOLINT(readability-simplify-boolean-expr)
            }
        }
        return true;
    }

    /**
     * @brief Extracts the blob_id from the given blob_file path.
     *
     * This function assumes that the file name is formatted as 16 hexadecimal digits
     * followed by the ".blob" extension. It extracts the first 16 characters as a hexadecimal number.
     *
     * @param path The blob_file path.
     * @return The extracted blob_id.
     * @note Behavior is undefined if the file name does not conform to the expected format.
     */
    [[nodiscard]] blob_id_type extract_blob_id(const boost::filesystem::path& path) const noexcept {
        std::string filename = path.filename().string();
        std::istringstream iss(filename.substr(0, 16));
        iss >> std::hex;
        blob_id_type id = 0;
        iss >> id;
        return id;
    }

    /**
     * @brief Returns the root directory from which blob_file_garbage_collector should start searching.
     *
     * @return The blob directory path.
     */
    [[nodiscard]] const boost::filesystem::path& get_blob_root() const noexcept {
        return blob_directory_;
    }

private:
    /**
     * @brief Precomputes all directory paths and stores them in the cache.
     */
    void precompute_directory_cache() noexcept {
        directory_cache_.reserve(directory_count_);
        for (std::size_t i = 0; i < directory_count_; ++i) {
            std::ostringstream dir_name;
            dir_name << "dir_" << std::setw(2) << std::setfill('0') << i;
            directory_cache_.emplace_back(blob_directory_ / dir_name.str());
        }
    }

    boost::filesystem::path blob_directory_;             ///< Full path to the `blob` directory.
    std::size_t directory_count_ = 100;                        ///< Number of directories for distribution.
    std::function<std::size_t(blob_id_type)> hash_function_; ///< Hash function to map blob_id to directory index.
    std::vector<boost::filesystem::path> directory_cache_; ///< Precomputed cache for directory paths.
};

} // namespace limestone::internal
