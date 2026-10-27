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
#include <vector>

#include <boost/filesystem.hpp>

#include "log_entry/log_entry_view.h"
#include "sort/sort_buffer.h"
#include "sort/sorting_directory.h"

namespace limestone::internal {

/**
 * @brief The sort of one worker thread. Accumulates entries in a buffer (sort_buffer), sorts and writes them out
 *        to a sorted file each time the buffer becomes full, and holds the list of the files written.
 *
 * Used from one thread (not thread-safe). The worker of the scan passes each data entry it reads to add(), and
 * calls finish() when it has exhausted the input. After finish(), the driver collects sorted_files() as the inputs
 * of the merge. The files are written by sorted_file_writer, so each is in the order of key_sid with duplicates
 * removed.
 */
class sort_worker {
public:
    /**
     * @brief Sets the size of the buffer and the place of the output. The arena is not allocated yet.
     * @param directory the place of the sorted files. It must be valid until this sort_worker is destroyed
     * @param arena_size_bytes the size of the arena in bytes. Writes a FATAL log and aborts if it is 0 or exceeds
     *                         sort_buffer::max_arena_size_bytes
     */
    sort_worker(sorting_directory& directory, std::size_t arena_size_bytes);

    ~sort_worker() = default;
    sort_worker(const sort_worker&) = delete;
    sort_worker& operator=(const sort_worker&) = delete;
    sort_worker(sort_worker&&) = delete;
    sort_worker& operator=(sort_worker&&) = delete;

    /**
     * @brief Puts one entry into the buffer.
     *
     * If the remaining space of the buffer is not enough, first sorts and writes out the contents of the buffer to
     * a sorted file and empties it, and then puts the entry. If the entry is larger than the whole arena, writes a
     * sorted file of that entry alone without going through the buffer (the contents of the buffer are unchanged).
     * @param entry a data entry. The bytes it refers to need to be valid only during the call (they are copied into
     *              the buffer). Writes a FATAL log and aborts if it is not a data entry, or if called after finish()
     * @throws limestone_io_exception if creating or writing a sorted file fails
     */
    void add(const log_entry_view& entry);

    /**
     * @brief Sorts and writes out the remainder of the buffer to a sorted file. Writes nothing if there is none.
     *
     * Called only once (writes a FATAL log and aborts if called twice).
     * @throws limestone_io_exception if creating or writing a sorted file fails
     */
    void finish();

    /**
     * @brief Returns the sorted files written, in the order written.
     * @return the list of the files. Valid until this sort_worker is destroyed
     */
    [[nodiscard]] const std::vector<boost::filesystem::path>& sorted_files() const noexcept;

private:
    /**
     * @brief Sorts and writes out the contents of the buffer to a sorted file, adds it to the list, and empties the
     *        buffer.
     * @throws limestone_io_exception if creating or writing the sorted file fails
     */
    void write_sorted_file();

    /**
     * @brief Writes a sorted file of one entry alone, and adds it to the list.
     * @param entry the entry to write
     * @throws limestone_io_exception if creating or writing the sorted file fails
     */
    void write_single_file(const log_entry_view& entry);

    sorting_directory* directory_;
    sort_buffer buffer_;
    std::vector<boost::filesystem::path> sorted_files_{};
    bool finished_{};
};

}  // namespace limestone::internal
