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

#include <cstdint>
#include <optional>

#include <boost/filesystem.hpp>

#include "log_entry/log_entry_reader.h"
#include "log_entry/log_entry_view.h"

namespace limestone::internal {

/**
 * @brief Reads a sorted file.
 *
 * Reads a sorted file (a sequence of data entries) from the beginning in order. With a start position, it can also
 * read a compacted WAL file skipping the marker_begin at its beginning.
 * Contents that do not match the format (the file ending in the middle of an entry, an invalid entry type, an entry
 * that is not a data entry) are reported by limestone_io_exception.
 */
class sorted_file_reader {
public:
    /**
     * @brief Opens the file and starts reading at the given position.
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @throws limestone_io_exception if the file cannot be opened (with errno), or if start_offset exceeds the size of
     *         the file (without errno)
     */
    explicit sorted_file_reader(boost::filesystem::path file, std::uint64_t start_offset = 0);

    /**
     * @brief Reads the next entry.
     *
     * The returned view is valid until next() is called next.
     * @return the next entry, or nullopt at the end of the file (and after that)
     * @throws limestone_io_exception if reading fails (with errno), or if the contents of the file do not match the
     *         format (without errno): the file ends in the middle of an entry, the entry type is invalid, or an entry
     *         is not a data entry
     */
    [[nodiscard]] std::optional<log_entry_view> next();

private:
    boost::filesystem::path file_;
    log_entry_reader reader_;
};

}  // namespace limestone::internal
