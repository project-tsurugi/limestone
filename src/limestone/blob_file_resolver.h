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
#include <functional>
#include <unordered_map>
#include <vector>

#include <limestone/api/blob_file.h>
#include <limestone/api/blob_pool.h>

namespace limestone::internal {

using limestone::api::blob_id_type; 
using limestone::api::blob_file;

/**
 * @brief Resolves file paths for given BLOB IDs with precomputed directory caching.
 */
class blob_file_resolver {
public:
    /**
     * @brief Constructs a blob_file_resolver with the given base directory.
     * 
     * The BLOB files are assumed to be stored under `<base_directory>/blob/`.
     * 
     * @param base_directory The base directory for storing BLOB files.
     * @param directory_count The number of subdirectories to distribute files into.
     * @param hash_function The function used to map `blob_id` to a directory index.
     */
    explicit blob_file_resolver(
        boost::filesystem::path base_directory,
        std::size_t directory_count = 100,
        std::function<std::size_t(blob_id_type)> hash_function = [](blob_id_type id) { return id; }) noexcept
        : blob_directory_(std::move(base_directory) / "blob"),
          directory_count_(directory_count),
          hash_function_(std::move(hash_function)) {
        // Precompute and cache all directory paths
        precompute_directory_cache();
    }

    /**
     * @brief Resolves the file path for the given BLOB ID.
     * 
     * @param blob_id The ID of the BLOB.
     * @return The resolved file path.
     */
    [[nodiscard]] boost::filesystem::path resolve_path(blob_id_type blob_id) const noexcept {
        // Calculate directory index
        std::size_t directory_index = hash_function_(blob_id) % directory_count_;

        // Retrieve precomputed directory path
        const boost::filesystem::path& subdirectory = directory_cache_[directory_index];

        // Generate the file name
        std::ostringstream file_name;
        file_name << std::hex << std::setw(16) << std::setfill('0') << blob_id << ".blob";

        return subdirectory / file_name.str();
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

    boost::filesystem::path blob_directory_;             // Full path to the `blob` directory
    std::size_t directory_count_;                        // Number of directories for distribution
    std::function<std::size_t(blob_id_type)> hash_function_; // Hash function to map blob_id to directory index

    std::vector<boost::filesystem::path> directory_cache_; // Precomputed cache for directory paths
};

} // namespace limestone::internal

