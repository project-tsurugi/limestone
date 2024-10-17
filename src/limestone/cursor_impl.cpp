/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include "cursor_impl.h"
#include <glog/logging.h>
#include "limestone_exception_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

std::unique_ptr<cursor> cursor_impl::create_cursor(const boost::filesystem::path& snapshot_file) {
    return std::unique_ptr<cursor>(new cursor(snapshot_file));
}

std::unique_ptr<cursor> cursor_impl::create_cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file) {
    return std::unique_ptr<cursor>(new cursor(snapshot_file, compacted_file));
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file) 
    : compacted_istrm_(std::nullopt) {
    open(snapshot_file, snapshot_istrm_);
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file) {
    open(snapshot_file, snapshot_istrm_);
    open(compacted_file, compacted_istrm_);
}

void cursor_impl::open(const boost::filesystem::path& file, std::optional<boost::filesystem::ifstream>& stream) {
    stream.emplace(file, std::ios_base::in | std::ios_base::binary);
    if (!stream->is_open() || !stream->good()) {
        LOG_AND_THROW_EXCEPTION("Failed to open file: " + file.string());
    }
}

void cursor_impl::close() {
    if (snapshot_istrm_) snapshot_istrm_->close();
    if (compacted_istrm_) compacted_istrm_->close();
}

void cursor_impl::validate_and_read_stream(std::optional<boost::filesystem::ifstream>& stream, const std::string& stream_name,
                                                std::optional<log_entry>& log_entry, std::string& previous_key_sid) {
    if (stream) {
        if (!stream->good()) {
            DVLOG_LP(log_trace) << stream_name << " stream is not good, closing it.";
            stream->close();
            stream = std::nullopt;
        } else if (stream->eof()) {
            DVLOG_LP(log_trace) << stream_name << " stream reached EOF, closing it.";
            stream->close();
            stream = std::nullopt;
        } else if (!log_entry) {
            log_entry.emplace();  // Construct a new log_entry
            if (!log_entry->read(*stream)) {
                stream->close();  // If reading fails, reset the stream
                stream = std::nullopt;
                log_entry = std::nullopt;  // Reset the log_entry as well
            } else {
                // Check if the key_sid is in ascending order
                // TODO: Key order violation is detected here and the process is aborted.
                // However, this check should be moved to an earlier point, and if the key order is invalid,
                // a different processing method should be considered instead of aborting immediately.
                if (!previous_key_sid.empty() && log_entry->key_sid() < previous_key_sid) {
                    LOG(ERROR) << "Key order violation in " << stream_name << ": current key_sid (" << log_entry->key_sid()
                               << ") is smaller than the previous key_sid (" << previous_key_sid << ")";
                    THROW_LIMESTONE_EXCEPTION("Key order violation detected in " + stream_name);
                }

                // Update the previous key_sid to the current one
                previous_key_sid = log_entry->key_sid();
            }
        }
    }
}

bool cursor_impl::next() {
    while (true) {
        // Only read from snapshot stream if snapshot_log_entry_ is empty, with key_sid check
        if (!snapshot_log_entry_) {
            validate_and_read_stream(snapshot_istrm_, "Snapshot", snapshot_log_entry_, previous_snapshot_key_sid);
        }

        // Only read from compacted stream if compacted_log_entry_ is empty, with key_sid check
        if (!compacted_log_entry_) {
            validate_and_read_stream(compacted_istrm_, "Compacted", compacted_log_entry_, previous_compacted_key_sid);
        }

        // Case 1: Both snapshot and compacted are empty, return false
        if (!snapshot_log_entry_ && !compacted_log_entry_) {
            DVLOG_LP(log_trace) << "Both snapshot and compacted streams are closed";
            return false;
        }

        // Case 2: Either snapshot or compacted has a value, use the one that is not empty
        if (snapshot_log_entry_ && !compacted_log_entry_) {
            log_entry_ = std::move(snapshot_log_entry_.value());
            snapshot_log_entry_ = std::nullopt;
        } else if (!snapshot_log_entry_ && compacted_log_entry_) {
            log_entry_ = std::move(compacted_log_entry_.value());
            compacted_log_entry_ = std::nullopt;
        } else {
            // Case 3: Both snapshot and compacted have values
            if (snapshot_log_entry_->key_sid() < compacted_log_entry_->key_sid()) {
                log_entry_ = std::move(snapshot_log_entry_.value());
                snapshot_log_entry_ = std::nullopt;
            } else if (snapshot_log_entry_->key_sid() > compacted_log_entry_->key_sid()) {
                log_entry_ = std::move(compacted_log_entry_.value());
                compacted_log_entry_ = std::nullopt;
            } else {
                // If key_sid is equal, use snapshot_log_entry_, but reset both entries
                log_entry_ = std::move(snapshot_log_entry_.value());
                snapshot_log_entry_ = std::nullopt;
                compacted_log_entry_ = std::nullopt;
            }
        }
        // Check if the current log_entry_ is a normal entry
        if (log_entry_.type() == log_entry::entry_type::normal_entry) {
            return true;
        }
        // If it's not a normal entry, continue the loop to skip it and read the next entry
    }
}

limestone::api::storage_id_type cursor_impl::storage() const noexcept {
    return log_entry_.storage();
}

void cursor_impl::key(std::string& buf) const noexcept {
    log_entry_.key(buf);
}

void cursor_impl::value(std::string& buf) const noexcept {
    log_entry_.value(buf);
}

log_entry::entry_type cursor_impl::type() const {
    return log_entry_.type();
}

} // namespace limestone::internal
