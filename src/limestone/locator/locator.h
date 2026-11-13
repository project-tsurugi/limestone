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
#include <vector>

#include <boost/filesystem.hpp>

#include "file_operations.h"

namespace limestone::internal {

/**
 * @brief A locator file that has been read. Holds the records and the size of the locatee file.
 *
 * The whole file is read at construction and is verified to be valid. It is invalid in the following 6 cases.
 * - The magic of the header does not match.
 * - The format version of the header is not 1.
 * - The size of the locator file minus 4096 and 16 is negative or is not a multiple of 8.
 * - The magic of the footer does not match.
 * - The size of the locatee file in the footer does not match the given size of the locatee file.
 * - The value of a record is at or beyond the size of the locatee file, or is at or below the value of the previous
 *   record (the records are not strictly ascending).
 */
class locator {
public:
    /**
     * @brief Reads the locator file and verifies that it is valid.
     * @param file the locator file
     * @param locatee_size the size of the locatee file (bytes) to compare with the value in the footer
     * @throws limestone_io_exception if the file cannot be opened or read (with errno)
     * @throws limestone_exception if the locator file is invalid
     */
    locator(boost::filesystem::path file, std::uint64_t locatee_size);

    /**
     * @brief Reads the locator file and verifies that it is valid, with the file operations replaced. For tests
     *        only; do not use it from production code.
     * @param file the locator file
     * @param locatee_size the size of the locatee file (bytes)
     * @param file_ops the file operations. Used only during the construction
     * @throws limestone_io_exception if the file cannot be opened or read (with errno)
     * @throws limestone_exception if the locator file is invalid
     */
    locator(boost::filesystem::path file, std::uint64_t locatee_size, file_operations& file_ops);

    /**
     * @brief Returns the number of records.
     */
    [[nodiscard]] std::size_t record_count() const noexcept;

    /**
     * @brief Returns the value of the k-th (0-based) record (a byte position from the beginning of the locatee file).
     * @param k the number of the record (0-based). Writes a FATAL log and aborts if it is record_count() or more
     * @return the byte position
     */
    [[nodiscard]] std::uint64_t position(std::size_t k) const;

    /**
     * @brief Returns the values of all records in the order of the record numbers.
     */
    [[nodiscard]] const std::vector<std::uint64_t>& positions() const noexcept;

    /**
     * @brief Returns the size of the locatee file recorded in the footer.
     */
    [[nodiscard]] std::uint64_t locatee_size() const noexcept;

    /**
     * @brief Returns the locator file that has been read.
     */
    [[nodiscard]] const boost::filesystem::path& file() const noexcept;

private:
    /**
     * @brief Reads and checks the locator file, and sets positions_ and locatee_size_. Called from the constructors
     */
    void read(file_operations& file_ops, std::uint64_t locatee_size);

    boost::filesystem::path file_;
    std::vector<std::uint64_t> positions_{};
    std::uint64_t locatee_size_{};
};

}  // namespace limestone::internal
