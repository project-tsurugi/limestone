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

#include <boost/filesystem.hpp>

#include "sort/buffered_output_file.h"

namespace limestone::internal {

/**
 * @brief Writes a locator file.
 *
 * Creates the file and writes the header at construction, receives the start positions of the data entries by
 * note_entry() as the locatee file is written, and writes the records according to the rule of the record
 * interval. close() writes the footer and closes the file (whether to fsync is specified at construction).
 * If destroyed without close(), an incomplete file remains (the caller removes it).
 *
 * Not thread-safe (use it from one thread).
 */
class locator_writer {
public:
    /**
     * @brief Creates the locator file and writes the header.
     * @param file the locator file. If it exists, it is truncated
     * @param record_interval_bytes the record interval (bytes). Writes a FATAL log and aborts if it is 0
     * @param sync_on_close whether close() performs fsync
     * @throws limestone_io_exception if the file cannot be created or the header cannot be written (with errno)
     */
    locator_writer(boost::filesystem::path file, std::uint64_t record_interval_bytes, bool sync_on_close);

    /**
     * @brief Closes the file if close() has not been called (failures are ignored).
     */
    ~locator_writer() = default;

    locator_writer(const locator_writer&) = delete;
    locator_writer& operator=(const locator_writer&) = delete;
    locator_writer(locator_writer&&) = delete;
    locator_writer& operator=(locator_writer&&) = delete;

    /**
     * @brief Receives the start position of a data entry, decides whether to record it, and writes the record.
     *
     * Holds the threshold of the next record. If offset is the threshold or more, records it and advances the
     * threshold to (offset / record interval + 1) * record interval. The initial threshold is 0, so the first
     * position received becomes record 0. An entry that crosses several multiples still yields one record.
     * @param offset the byte position of the entry from the beginning of the locatee file. Writes a FATAL log and
     *               aborts if it is less than the previous position, or if called after close()
     * @throws limestone_io_exception if the record cannot be written (with errno)
     */
    void note_entry(std::uint64_t offset);

    /**
     * @brief Writes the footer, performs fsync if specified at construction, and closes the file. Writes a FATAL
     *        log and aborts if called after close().
     * @param locatee_size the size of the locatee file (bytes). Writes a FATAL log and aborts if it is less than
     *                     the last position passed to note_entry()
     * @throws limestone_io_exception if the footer cannot be written, or fflush, fsync or fclose fails (with errno)
     */
    void close(std::uint64_t locatee_size);

    /**
     * @brief Returns the number of records written so far.
     */
    [[nodiscard]] std::size_t record_count() const noexcept;

    /**
     * @brief Reads an existing locatee file from the beginning and generates its locator file.
     *
     * Passes the start positions of the data entries to note_entry(), and calls close() at the end (with fsync).
     * When the same key continues, the second and later entries are not passed (a record points at the first
     * occurrence of a key).
     * The content is the same as the one generated while the locatee file is written (whose entries have no
     * duplicate keys).
     * @param locatee the locatee file. Its entries must be contiguous from the beginning to the end
     * @param file the locator file to generate. If it exists, it is truncated
     * @param record_interval_bytes the record interval (bytes)
     * @return the number of records written
     * @throws limestone_io_exception if the locatee file cannot be read, its content does not conform to the
     *         format, or the locator file cannot be written. An incomplete locator file then remains (the caller
     *         removes it)
     */
    static std::size_t build_from_file(const boost::filesystem::path& locatee, const boost::filesystem::path& file,
                                       std::uint64_t record_interval_bytes);

private:
    buffered_output_file file_;
    std::uint64_t record_interval_bytes_;
    bool sync_on_close_;
    // The threshold of the position at which the next record is written
    std::uint64_t next_threshold_{};
    // The position passed to the previous note_entry(). Used to check that the position does not go back
    std::uint64_t last_offset_{};
    std::size_t record_count_{};
};

}  // namespace limestone::internal
