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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

namespace limestone::internal::locator_format {

/// The length of the header (bytes)
constexpr std::size_t header_size = 4096;

/// The length of a record (bytes). Holds a byte position from the beginning of the locatee file in little endian
constexpr std::size_t record_size = 8;

/// The length of the footer (bytes). The size of the locatee file (8 bytes) and the magic of the footer (8 bytes)
constexpr std::size_t footer_size = 16;

/// The magic of the header. ASCII "LIMELOC" followed by 0x00, read as a little-endian integer
constexpr std::uint64_t header_magic = 0x00434F4C454D494CULL;

/// The magic of the footer. The magic of the header with the most significant bit set to 1
constexpr std::uint64_t footer_magic = 0x80434F4C454D494CULL;

/// The format version of the locator file. The reader treats any other value as invalid
constexpr std::uint64_t format_version = 1;

/// The suffix of the name of a locator file
constexpr std::string_view suffix = ".loc";

/**
 * @brief Makes the name of the locator file from the name of the locatee file.
 * @param locatee_name the name of the locatee file
 * @return the name with .loc appended
 */
[[nodiscard]] std::string locator_file_name(std::string_view locatee_name);

/**
 * @brief Makes the name of the locatee file from the name of the locator file.
 * @param locator_name the name of the locator file (must end with .loc; otherwise writes a FATAL log and aborts)
 * @return the name with .loc removed
 */
[[nodiscard]] std::string locatee_file_name(std::string_view locator_name);

/**
 * @brief Makes the path of the locator file from the path of the locatee file: the same directory, and the name
 *        with .loc appended.
 * @param locatee the path of the locatee file
 * @return the path of the locator file
 */
[[nodiscard]] boost::filesystem::path locator_file_path(const boost::filesystem::path& locatee);

/**
 * @brief Returns whether the name is the name of a locator file (ending with .loc).
 * @param name the name of the file (without the directory)
 * @return true if it ends with .loc
 */
[[nodiscard]] bool is_locator_file_name(std::string_view name);

}  // namespace limestone::internal::locator_format
