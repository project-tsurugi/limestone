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
#include <limestone/api/blob_id_type.h>
#include <limestone/api/cursor.h>
#include <limestone/api/storage_id_type.h>

#include <boost/filesystem.hpp>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "cursor_impl_base.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_reader.h"
#include "log_entry/log_entry_view.h"
#include "region_allocator.h"

namespace limestone::internal {

using limestone::api::cursor;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;    
class cursor_impl : public cursor_impl_base {
public:
    cursor_impl(const boost::filesystem::path& snapshot_file, std::map<storage_id_type, write_version_type> clear_storage);

    cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                std::map<storage_id_type, write_version_type> clear_storage);

    /**
     * @brief Creates a cursor that reads the regions handed out by the region allocator.
     *
     * The files are opened here. The regions are taken in next(): each time nothing is left to read, the cursor asks
     * the allocator for the next region, until none is left. The allocator is shared with the other cursors, so that
     * all the cursors together read each region once.
     * @param snapshot_file the snapshot file
     * @param compacted_file the compacted WAL file, or nullopt if there is none (it must agree with the locators of
     *                       the allocator)
     * @param clear_storage the write version per storage below which entries are skipped
     * @param allocator the region allocator. Writes a FATAL log and aborts if it is empty
     * @throws limestone_io_exception if a file cannot be opened
     */
    cursor_impl(const boost::filesystem::path& snapshot_file, const std::optional<boost::filesystem::path>& compacted_file,
                std::map<storage_id_type, write_version_type> clear_storage, std::shared_ptr<region_allocator> allocator);

    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);
    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);
    /**
     * @brief Creates a cursor that reads the regions handed out by the region allocator, and wraps it in api::cursor.
     *        The arguments are the same as those of the constructor with the same arguments.
     */
    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file,
                                                 const std::optional<boost::filesystem::path>& compacted_file,
                                                 const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage,
                                                 std::shared_ptr<region_allocator> allocator);
protected:
    /// The state of reading one file
    struct input_stream {
        /// the name of the kind of the file, used in messages
        std::string name{};
        /// the reader of the file. Empty if there is no file to read
        std::optional<log_entry_reader> reader{};
        /// whether the current region has been read to its end. If true, nothing is read from the reader until the
        /// next region is taken
        bool finished{true};
        /// a view of the entry read and not returned yet. It refers to the buffer of the reader, and is valid until
        /// the reader reads next
        std::optional<log_entry_view> entry{};
        /// the key_sid of the entry read just before, used for the check of the ascending order
        std::string previous_key_sid{};
    };

private:
    /// the current entry (the one returned by the last next()). A view into the buffer of the reader it was read
    /// from; the reader is not read again before next() is called next, so the view is valid until then
    log_entry_view current_{};
    input_stream snapshot_{"Snapshot"};
    input_stream compacted_{"Compacted"};
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage_; 
    /// the region allocator. Empty in a cursor created without an allocator (one that reads the whole files)
    std::shared_ptr<region_allocator> allocator_{};
    /// the start key of the current region. Entries below it are skipped. If none, nothing is skipped
    std::optional<std::string> start_key_{};
    /// the end key of the current region. When an entry at or above it is reached, the current region of that file
    /// has been read to its end. If none, the file is read up to the end position of the region
    std::optional<std::string> end_key_{};

    /**
     * @brief Asks the allocator for the next region, and if there is one, sets the read ranges of the readers and the
     *        key range to it.
     * @return true if there is a next region. false if there is no allocator (a cursor that reads without regions),
     *         none is left, or the cursor is closed
     * @throws limestone_exception if a probe of the allocator fails (the locator is invalid)
     * @throws limestone_io_exception if reading fails
     */
    bool acquire_next_range();

protected:
    void close() override;

    bool next() override;

    /**
     * @brief Reads one entry from the file as the next candidate to return, and puts it in stream.entry.
     *
     * Skips entries that are not data entries, entries with the same key as the previous one, entries older than the
     * boundary of clear_storage, and entries below the start key. When the end position of the region is reached, or
     * an entry at or above the end key is reached (the current region has been read to its end), sets stream.finished
     * to true and leaves stream.entry empty. Does nothing if stream.finished is true.
     * @param stream the reading state
     * @throws limestone_exception if the contents of the file do not match the format, or the keys are not in
     *         ascending order
     * @throws limestone_io_exception if reading fails
     */
    void validate_and_read_stream(input_stream& stream);

    [[nodiscard]] limestone::api::storage_id_type storage() const noexcept override;
    void key(std::string& buf) const noexcept override;
    void value(std::string& buf) const noexcept override;
    [[nodiscard]] std::vector<limestone::api::blob_id_type> blob_ids() const override;
    [[nodiscard]] limestone::api::log_entry::entry_type type() const override;
    bool is_relevant_entry(const log_entry_view& entry);
    // Making the cursor class a friend so that it can access protected members
    friend class limestone::api::cursor;
};

} // namespace limestone::internal
