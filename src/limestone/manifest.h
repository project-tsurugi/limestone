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
#include <boost/optional.hpp>
#include <nlohmann/json.hpp>
#include <string_view>

#include "file_operations.h"

namespace limestone::internal {

class manifest {
public:
    // Manifest file names as static constants
    static constexpr std::string_view file_name        = "limestone-manifest.json";
    static constexpr std::string_view backup_file_name = "limestone-manifest.json.back";

    static constexpr const char *version_error_prefix = "/:limestone unsupported dbdir persistent format version: "
            "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/upgrade-guide.md";

    /**
     * @brief Default format version for new manifest files.
     * @note Update this value when upgrading the manifest format version.
     */
    static constexpr const char* default_format_version = "1.1";

    /**
     * @brief Default persistent format version for new manifest files.
     * @note Update this value when upgrading the manifest persistent format version.
     */
    static constexpr int default_persistent_format_version = 5;

    /**
     * @brief Constructs a manifest object with the default version information.
     *
     * Initializes the manifest using the current default format version and persistent format version.
     * These defaults are defined as static constexpr members of this class.
     */
    manifest();

    /**
     * @brief Constructs a manifest object with specified values (for testing or advanced use).
     *
     * This constructor allows explicit setting of all manifest fields for testing or special purposes.
     *
     * @param format_version           The format version string.
     * @param persistent_format_version The persistent format version as integer.
     * @param instance_uuid            The instance UUID string (RFC 4122 version 4 format).
     *
     * @note This constructor is intended primarily for unit tests or advanced usage. 
     *       Production code should normally use the default constructor and standard initialization methods.
     */
    manifest(std::string format_version, int persistent_format_version, std::string instance_uuid);


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

    /**
     * @brief Converts the current object state to a JSON formatted string.
     *
     * This function serializes the object's internal state into a JSON string,
     * allowing for easy data interchange or logging in a standardized format.
     *
     * @return std::string The JSON representation of the object's state.
     */
    [[nodiscard]] std::string to_json_string() const;

    /**
     * @brief Constructs a manifest object from a JSON string.
     *
     * This function parses the provided JSON-formatted string and converts it into
     * a manifest object. It ensures that the JSON string adheres to the expected format
     * for a manifest, performing necessary validations and transformations.
     *
     * @param json_str A valid JSON string representing the manifest.
     * @return A manifest object populated with data extracted from the JSON string.
     * @throws std::runtime_error if the JSON parsing fails or if the JSON does not represent a valid manifest.
     */
    [[nodiscard]] static manifest from_json_string(const std::string& json_str);

    /**
     * @brief Returns the format version of the manifest.
     *
     * @return const reference to the format version string.
     */
    [[nodiscard]] const std::string& get_format_version() const;

    /**
     * @brief Returns the persistent format version of the manifest.
     *
     * @return persistent format version as integer.
     */
    [[nodiscard]] int get_persistent_format_version() const;

    /**
     * @brief Returns the instance UUID of the manifest.
     *
     * @return const reference to the instance UUID string (RFC 4122 version 4 format).
     */
    [[nodiscard]] const std::string& get_instance_uuid() const;

    /**
     * @brief Loads a manifest object from the specified file path.
     *
     * This function attempts to read and parse a manifest file from the given path.
     * If the file does not exist or parsing fails, boost::none is returned.
     * On success, a valid manifest object is returned via boost::optional.
     *
     * @param path The file system path to the manifest file.
     * @param ops  The file operations implementation to use (e.g., real or test).
     * @return boost::optional<manifest>  The manifest object if the file exists and is valid, boost::none otherwise.
     */
    static boost::optional<manifest> load_manifest_from_path(const boost::filesystem::path& path, file_operations& ops);

protected:
    /**
     * @brief Internal helper for testing: checks whether a file exists using the specified file_operations.
     *
     * This overload is intended solely for unit tests or test-specific stubs.
     * Production code should always use exists_path(const boost::filesystem::path& path).
     *
     * @param path The file path to check.
     * @param ops  The file_operations implementation to use (e.g. a test stub).
     * @return true if the file exists, false otherwise.
     * @throws limestone_io_exception on I/O errors (other than file not found).
     *
     * @note This function is protected and exists only for testing purposes.
     *       Do not call it from production code except in tests.
     */
    static bool exists_path_with_ops(const boost::filesystem::path& path, file_operations& ops);

private:
    static bool exists_path(const boost::filesystem::path& path);
    static void write_file_safely(const boost::filesystem::path& file_path, const manifest& m, file_operations& ops); // safe atomic write for manifest
    static void migrate_manifest(const boost::filesystem::path& manifest_path, const boost::filesystem::path& manifest_backup_path,
                                 const manifest& old_manifest, file_operations& ops);
    static std::string generate_instance_uuid();

    std::string format_version_;
    int persistent_format_version_;
    std::string instance_uuid_;
};


} // namespace limestone::internal
