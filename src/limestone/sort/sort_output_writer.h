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
#include <string>

#include <boost/filesystem.hpp>

#include "locator/locator_writer.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_view.h"
#include "sort/buffered_output_file.h"

namespace limestone::internal {

/**
 * @brief Writes the output of the sort (the snapshot file or the compacted WAL file).
 *
 * The output begins with a marker_begin of epoch 0, followed by data entries. The result of the final merge is passed
 * to write() entry by entry (the order is not checked and duplicates are not removed). How the write_version is
 * written, whether remove_entries are written, and whether fsync is performed at close are specified by format.
 *
 * If the output of the locator file is specified, the locator file is also generated as the file is written.
 *
 * Not thread-safe (used from one thread).
 */
class sort_output_writer {
public:
    /// the format of the output
    struct format {
        /// how the write_version is written
        limestone::api::log_entry::write_version_handling write_version;
        /// whether remove_entries are written. If false, the remove_entries passed to write() are not written
        bool keep_remove_entry;
        /// whether close() performs fsync
        bool sync_on_close;
    };

    /// The output of the locator file
    struct locator_output {
        /// The locator file. If it exists, it is truncated
        boost::filesystem::path file;
        /// The record interval (bytes)
        std::uint64_t record_interval_bytes;
    };

    /**
     * @brief Creates and opens the file, and writes a marker_begin of epoch 0.
     *
     * If the output of the locator file is specified, also creates the locator file and writes its header.
     * @param file the file to write. If it exists, it is truncated
     * @param label the name of the kind of the file used in error messages (for example "snapshot file")
     * @param output_format the format of the output
     * @param locator the output of the locator file. If std::nullopt, no locator file is generated. Writes a FATAL
     *                log and aborts if the record interval is 0
     * @throws limestone_io_exception if the file cannot be opened or the marker_begin cannot be written (with errno).
     *         The same applies if the locator file cannot be created or its header cannot be written
     */
    sort_output_writer(boost::filesystem::path file, std::string label, const format& output_format,
                       const std::optional<locator_output>& locator);

    /**
     * @brief Closes the file if close() has not been called (a failure is ignored).
     */
    ~sort_output_writer() = default;

    sort_output_writer(const sort_output_writer&) = delete;
    sort_output_writer& operator=(const sort_output_writer&) = delete;
    sort_output_writer(sort_output_writer&&) = delete;
    sort_output_writer& operator=(sort_output_writer&&) = delete;

    /**
     * @brief Writes the entry according to format.
     *
     * When the locator file is output, the start position of the entry (the bytes written so far) is recorded in
     * the locator file according to the rule of the record interval before the entry is written. A remove_entry
     * that format does not write is not recorded.
     * @param entry a data entry. Writes a FATAL log and aborts if it is not a data entry. Also writes a FATAL log and
     *              aborts if called after close()
     * @throws limestone_io_exception if writing fails (with errno). The same applies if a record of the locator
     *         file cannot be written
     */
    void write(const log_entry_view& entry);

    /**
     * @brief Finishes writing and closes the file (fflush, fsync if sync_on_close of format, fclose).
     *
     * The file is closed even on failure. If called after the file is closed, writes a WARNING log and does nothing.
     * However, with sync_on_close, if the previous close() failed and the contents are not fsynced, writes a FATAL
     * log and aborts.
     *
     * When the locator file is output, after the file is closed, the bytes written are written to the footer and
     * the locator file is closed (whether to fsync follows sync_on_close). If closing the file fails, the locator
     * file is not closed, and the second and later calls do nothing to the locator file. The locator file then
     * remains incomplete (the caller removes it).
     * @throws limestone_io_exception if fflush, fsync or fclose fails (with errno). The same applies if the footer
     *         of the locator file cannot be written or the locator file cannot be closed
     */
    void close();

private:
    buffered_output_file file_;
    format format_;
    // The writer of the locator file. Held only when the output is specified
    std::optional<locator_writer> locator_{};
    // The bytes written so far (including the marker_begin): the start position of the next entry to write
    std::uint64_t written_bytes_{log_entry_format::marker_size};
};

}  // namespace limestone::internal
