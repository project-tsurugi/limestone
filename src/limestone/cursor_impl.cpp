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
#include <map>
#include "cursor_impl.h"
#include <glog/logging.h>
#include "limestone_exception_helper.h"
#include "log_entry/log_entry_format.h"

namespace limestone::internal {

using limestone::api::log_entry;
using limestone::api::write_version_type;

std::unique_ptr<cursor> cursor_impl::create_cursor(
    const boost::filesystem::path& snapshot_file,
    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) {

    auto impl = std::make_unique<cursor_impl>(snapshot_file, clear_storage);
    return std::unique_ptr<cursor>(new cursor(std::move(impl)));
}

std::unique_ptr<cursor> cursor_impl::create_cursor(
    const boost::filesystem::path& snapshot_file,
    const boost::filesystem::path& compacted_file,
    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) {

    auto impl = std::make_unique<cursor_impl>(snapshot_file, compacted_file, clear_storage);
    return std::unique_ptr<cursor>(new cursor(std::move(impl)));
}

std::unique_ptr<cursor> cursor_impl::create_cursor(
    const boost::filesystem::path& snapshot_file,
    const std::optional<boost::filesystem::path>& compacted_file,
    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage,
    std::shared_ptr<region_allocator> allocator) {

    std::unique_ptr<cursor_impl> impl = std::make_unique<cursor_impl>(snapshot_file, compacted_file, clear_storage, std::move(allocator));
    return std::unique_ptr<cursor>(new cursor(std::move(impl)));
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file, std::map<api::storage_id_type, api::write_version_type> clear_storage)
    : clear_storage_(std::move(clear_storage)) {
    // log_entry_reader throws limestone_io_exception if the file cannot be opened
    snapshot_.reader.emplace(snapshot_file);
    snapshot_.finished = false;
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                         std::map<api::storage_id_type, api::write_version_type> clear_storage)
    : clear_storage_(std::move(clear_storage)) {
    // log_entry_reader throws limestone_io_exception if the file cannot be opened
    snapshot_.reader.emplace(snapshot_file);
    snapshot_.finished = false;
    compacted_.reader.emplace(compacted_file);
    compacted_.finished = false;
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file,
                         const std::optional<boost::filesystem::path>& compacted_file,
                         std::map<api::storage_id_type, api::write_version_type> clear_storage,
                         std::shared_ptr<region_allocator> allocator)
    : clear_storage_(std::move(clear_storage)), allocator_(std::move(allocator)) {
    if (! allocator_) {
        LOG_LP(FATAL) << "no region allocator";
    }
    // log_entry_reader throws limestone_io_exception if the file cannot be opened.
    // keep the streams finished until a region is taken
    snapshot_.reader.emplace(snapshot_file);
    if (compacted_file.has_value()) {
        compacted_.reader.emplace(*compacted_file);
    }
}

void cursor_impl::close() {
    snapshot_.reader = std::nullopt;
    snapshot_.finished = true;
    compacted_.reader = std::nullopt;
    compacted_.finished = true;
}

void cursor_impl::validate_and_read_stream(input_stream& stream) {
    while (! stream.finished && stream.reader.has_value()) {
        const auto read_result = stream.reader->next();
        if (read_result.result() == log_entry_read_result::status::end) {
            DVLOG_LP(log_trace) << stream.name << " file reached the end.";
            stream.finished = true;
            return;
        }
        if (read_result.result() == log_entry_read_result::status::error) {
            LOG_AND_THROW_EXCEPTION(stream.name + " file is broken: " + stream.reader->file().string() + " at offset "
                                    + std::to_string(read_result.offset()) + ": " + read_result.error().message());
        }
        const auto view = read_result.view();
        if (! log_entry_format::is_data_entry(view.type())) {
            continue;
        }
        const std::string_view key_sid = view.serialized_key_sid();
        // the key range of the region: skip entries below the start key, and stop at an entry at or above the end key
        if (start_key_.has_value() && log_entry_format::compare_key_sid(key_sid, *start_key_) < 0) {
            continue;
        }
        if (end_key_.has_value() && log_entry_format::compare_key_sid(key_sid, *end_key_) >= 0) {
            DVLOG_LP(log_trace) << stream.name << " file reached the end key of the region.";
            stream.finished = true;
            return;
        }
        // Check if the key_sid is in ascending order
        // TODO: Key order violation is detected here and the process is aborted.
        // However, this check should be moved to an earlier point, and if the key order is invalid,
        // a different processing method should be considered instead of aborting immediately.
        if (!stream.previous_key_sid.empty()) {
            const int order = log_entry_format::compare_key_sid(key_sid, stream.previous_key_sid);
            if (order < 0) {
                LOG(ERROR) << "Key order violation in " << stream.name << ": current key_sid (" << key_sid
                           << ") is smaller than the previous key_sid (" << stream.previous_key_sid << ")";
                THROW_LIMESTONE_EXCEPTION("Key order violation detected in " + stream.name);
            }
            // Skip processing if key_sid is the same as stream.previous_key_sid
            if (order == 0) {
                DVLOG_LP(log_trace_fine) << stream.name << " log entry key_sid (" << key_sid
                                         << ") is same as previous, skipping.";
                continue;
            }
        }
        // Update the previous key_sid to the current one
        stream.previous_key_sid.assign(key_sid);

        // keep the view of a relevant entry. The reader is not read again until the entry is consumed, so the view
        // stays valid
        if (is_relevant_entry(view)) {
            stream.entry = view;
            return;
        }
    }
}


bool cursor_impl::is_relevant_entry(const log_entry_view& entry) {
        // Step 1: Check if the entry is one of normal_entry, remove_entry, or normal_with_blob
        if (! log_entry_format::is_data_entry(entry.type())) {
            return false;  // Skip this entry if it's not a normal, remove, or blob entry
        }


        // Step 2: Get the storage ID from log_entry
        auto storage_id = entry.storage();  // Assuming storage() returns the storage ID
        // Step 3: Check if clear_storage_ contains the same storage ID
        auto it = clear_storage_.find(storage_id);
        if (it != clear_storage_.end()) {
            // Step 4: Retrieve the write_version from log_entry (only if the storage ID is found)
            write_version_type wv = entry.write_version();

            // Step 5: Retrieve the write_version from clear_storage_ for the same storage ID
            write_version_type range_ver = it->second;

            // Step 6: Compare the versions
            if (wv < range_ver) {
                return false;  // Skip this entry as it is outdated
            }
        }

        // If everything is valid, return true
        return true;
}

bool cursor_impl::next() { 
    while (true) {
        // Read from the snapshot stream if the snapshot_.entry is empty
        if (!snapshot_.entry) {
            validate_and_read_stream(snapshot_);
        }

        // Read from the compacted stream if the compacted_.entry is empty
        if (!compacted_.entry) {
            validate_and_read_stream(compacted_);
        }

        // Case 1: Both snapshot and compacted are empty. Continue if there is a next region, otherwise return false
        if (!snapshot_.entry && !compacted_.entry) {
            if (acquire_next_range()) {
                continue;
            }
            DVLOG_LP(log_trace) << "Both snapshot and compacted streams are closed";
            return false;
        }

        // Case 2: Either snapshot or compacted has a value, use the one that is not empty
        if (snapshot_.entry && !compacted_.entry) {
            current_ = *snapshot_.entry;
            snapshot_.entry = std::nullopt;
        } else if (!snapshot_.entry && compacted_.entry) {
            current_ = *compacted_.entry;
            compacted_.entry = std::nullopt;
        } else {
            // Case 3: Both snapshot and compacted have values
            const int order = log_entry_format::compare_key_sid(snapshot_.entry->serialized_key_sid(),
                                                                compacted_.entry->serialized_key_sid());
            if (order < 0) {
                current_ = *snapshot_.entry;
                snapshot_.entry = std::nullopt;
            } else if (order > 0) {
                current_ = *compacted_.entry;
                compacted_.entry = std::nullopt;
            } else {
                // If key_sid is equal, snapshot is always newer by design
                // Note: If snapshot contains a remove_entry, it will be filtered out 
                // by the type check at the end of the method
                current_ = *snapshot_.entry;
                snapshot_.entry = std::nullopt;
                compacted_.entry = std::nullopt;
            }
        }

        // Check if the current entry is a normal entry or normal_with_blob
        if (current_.type() == log_entry::entry_type::normal_entry ||
            current_.type() == log_entry::entry_type::normal_with_blob) {
            return true;
        }

        // If it's not a normal entry, continue the loop to skip it and read the next entry
    }
}



bool cursor_impl::acquire_next_range() {
    if (! allocator_ || ! snapshot_.reader.has_value()) {
        return false;
    }
    log_entry_reader* compacted_reader = compacted_.reader.has_value() ? &*compacted_.reader : nullptr;
    std::optional<region_range> range = allocator_->allocate(*snapshot_.reader, compacted_reader);
    if (! range.has_value()) {
        return false;
    }
    snapshot_.reader->set_range(range->snapshot_start, range->snapshot_end);
    snapshot_.finished = false;
    snapshot_.previous_key_sid.clear();
    if (compacted_reader != nullptr) {
        compacted_reader->set_range(range->compacted_start, range->compacted_end);
        compacted_.finished = false;
        compacted_.previous_key_sid.clear();
    }
    start_key_ = std::move(range->start_key);
    end_key_ = std::move(range->end_key);
    return true;
}

limestone::api::storage_id_type cursor_impl::storage() const noexcept {
    return current_.storage();
}

void cursor_impl::key(std::string& buf) const noexcept {
    buf.assign(current_.key());
}

void cursor_impl::value(std::string& buf) const noexcept {
    buf.assign(current_.value());
}

log_entry::entry_type cursor_impl::type() const {
    return current_.type();
}

std::vector<limestone::api::blob_id_type> cursor_impl::blob_ids() const {
    return current_.blob_ids();
}

} // namespace limestone::internal
