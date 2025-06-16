#pragma once

#include <string>

namespace limestone::replication {

/**
 * @brief Enum for async replication mode.
 */
enum class async_replication {
    disabled,           ///< Synchronous mode (default)
    std_async,          ///< Use std::async for asynchronous operation
    single_thread_async ///< Asynchronous operation in a single thread
};

/**
 * @brief Converts async_replication enum to string.
 */
std::string to_string(async_replication mode);

/**
 * @brief Converts string to async_replication enum.
 * @param str The string to convert.
 * @return The corresponding async_replication enum value.
 * @note Throws std::invalid_argument if the string is invalid.
 */
async_replication async_replication_from_string(const std::string& str);

/**
 * @brief Parses the specified environment variable and returns the corresponding async_replication value.
 * @param env_name The environment variable name to check.
 * @return The corresponding async_replication enum value.
 * @note If the value is invalid, this function logs a fatal error and terminates the process.
 */
async_replication async_replication_from_env(const char* env_name);

} // namespace limestone::replication
