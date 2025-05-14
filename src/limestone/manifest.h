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

#include <boost/filesystem.hpp>
#include <nlohmann/json.hpp>
#include <string_view>

#include "file_operations.h"

namespace limestone::internal {

class manifest {
public:
    manifest() = default;

    // Manifest file names as static constants
    static constexpr std::string_view file_name        = "limestone-manifest.json";
    static constexpr std::string_view backup_file_name = "limestone-manifest.json.back";

    static constexpr const char *version_error_prefix = "/:limestone unsupported dbdir persistent format version: "
            "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/upgrade-guide.md";

    /**
     * @brief Initializes the manifest.
     *
     * @param logdir The path specifying the directory to be used for logging.
     */
    static void create_initial(const boost::filesystem::path& logdir);

    /**
     * @brief Initializes the manifest, with an injectable file-ops backend.
     *
     * @note  This overload is intended for unit tests or special environments
     *        where the behavior of file operations must be stubbed or mocked.
     *
     * @param logdir The path specifying the directory to be used for logging.
     * @param ops    Custom file_operations implementation (e.g. a test stub).
     */
    static void create_initial(const boost::filesystem::path& logdir, file_operations& ops);

    /**
     * @brief Acquires an exclusive lock on the manifest file.
     *
     * @param logdir The path to the log directory containing the manifest file.
     * @return The file descriptor on success; returns -1 on failure, with errno set.
     *
     * @note This function does not log errors or handle them internally.
     *       The caller must check the return value and inspect errno for error details.
     */
    static int acquire_lock(const boost::filesystem::path& logdir);


    /**
     * @brief Acquires an exclusive lock on the manifest file, with injectable I/O backend.
     *
     * @note This overload is intended for unit tests or special environments
     *       where the file-opening and locking behavior must be stubbed or mocked.
     *
     * @param logdir The path to the log directory containing the manifest file.
     * @param ops    Custom file_operations implementation (e.g. a test stub).
     * @return The file descriptor on success; returns -1 on failure, with errno set.
     */
    static int acquire_lock(const boost::filesystem::path& logdir, file_operations& ops);

    /**
     * @brief Checks whether the manifest at the given path has a supported format version.
     *
     * @param manifest_path Path to the manifest file to validate.
     * @param errmsg Reference to a string to receive an error message if validation fails or is unsupported.
     * @return Positive integer (>=1) if supported format version; 0 if unsupported version; negative integer on parse or I/O error.
     */
    static int is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg);


    /**
     * @brief Validates the manifest file in the specified log directory and performs repair or migration if necessary.
     *
     * @param logdir Path to the log directory containing the manifest file.
     */
    static void check_and_migrate(const boost::filesystem::path& logdir);

    /**
     * @brief Validates and migrates the manifest file, with injectable I/O backend.
     *
     * @note This overload is intended for unit tests or special environments
     *       where file-system operations (rename/remove) must be stubbed or mocked.
     *
     * @param logdir Path to the log directory containing the manifest file.
     * @param ops    Custom file_operations implementation (e.g. a test stub).
     */ 
    static void check_and_migrate(const boost::filesystem::path& logdir, file_operations& ops);


private:
    static bool exists_path(const boost::filesystem::path& path);
};

} // namespace limestone::internal
