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
#include <cstdio>
#include <string>

#include <boost/filesystem.hpp>

#include "log_entry/log_entry_view.h"
#include "sort/key_sid_deduplicator.h"

namespace limestone::internal {

/**
 * @brief Writes a sorted file.
 *
 * A sorted file is a sequence of data entries. It has no marker, the write_versions are written as they are, and
 * remove_entries are kept. The entries are passed to write() in ascending order of key_sid (and in descending order
 * of write_version within the same key_sid). Only the first of the entries with the same key_sid is written
 * (duplicates are removed).
 */
class sorted_file_writer {
public:
    /// the size of the stdio buffer in bytes. The fwrite of each entry is accumulated up to this size before write
    static constexpr std::size_t stream_buffer_size = std::size_t{128} * 1024;

    /**
     * @brief Creates and opens the file.
     * @param file the file to write. If it exists, it is truncated
     * @throws limestone_io_exception if the file cannot be opened (with errno)
     */
    explicit sorted_file_writer(boost::filesystem::path file);

    /**
     * @brief Closes the file if close() has not been called (without fsync; a failure is ignored).
     */
    ~sorted_file_writer();

    sorted_file_writer(const sorted_file_writer&) = delete;
    sorted_file_writer& operator=(const sorted_file_writer&) = delete;
    sorted_file_writer(sorted_file_writer&&) = delete;
    sorted_file_writer& operator=(sorted_file_writer&&) = delete;

    /**
     * @brief Writes the entry. Does not write it if its key_sid is the same as that of the entry written last.
     * @param entry a data entry. Writes a FATAL log and aborts if it is not a data entry, or if its key_sid is
     *              smaller than that of the entry written last. Also writes a FATAL log and aborts if called after
     *              close()
     * @throws limestone_io_exception if writing fails (with errno)
     */
    void write(const log_entry_view& entry);

    /**
     * @brief Finishes writing, makes the contents reach the disk, and closes the file (fflush, fsync, fclose).
     *
     * The file is closed even on failure. Writes a FATAL log and aborts if called twice.
     * @throws limestone_io_exception if fflush, fsync or fclose fails (with errno)
     */
    void close();

    /**
     * @brief Returns the number of entries written (excluding the duplicates removed).
     */
    [[nodiscard]] std::uint64_t written_count() const noexcept;

private:
    boost::filesystem::path file_;
    /// the stdio buffer. Owned here because glibc ignores the size given to setvbuf without a buffer
    std::string stream_buffer_;
    FILE* stream_{};
    key_sid_deduplicator deduplicator_{};
    std::uint64_t written_count_{};
};

}  // namespace limestone::internal
