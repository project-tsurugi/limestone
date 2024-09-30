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
#include <limestone/api/cursor.h>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "log_entry.h"
#include <limestone/api/write_version_type.h>

namespace limestone::api {

// snapshot_tracker handles the retrieval and comparison of log entries from snapshot and compacted streams.
// It ensures that the entries are read in the correct order and manages the state of both streams.
class snapshot_tracker {

private:
    log_entry log_entry_;
    std::optional<log_entry> snapshot_log_entry_;
    std::optional<log_entry> compacted_log_entry_;
    std::optional<boost::filesystem::ifstream> snapshot_istrm_;
    std::optional<boost::filesystem::ifstream> compacted_istrm_;
    std::string previous_snapshot_key_sid;
    std::string previous_compacted_key_sid;

public:
    explicit snapshot_tracker(const boost::filesystem::path& snapshot_file)
        : compacted_istrm_(std::nullopt) {
        open(snapshot_file, snapshot_istrm_);
    }

    explicit snapshot_tracker(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file) {
        open(snapshot_file, snapshot_istrm_);
        open(compacted_file, compacted_istrm_);
    }

    void open(const boost::filesystem::path& file, std::optional<boost::filesystem::ifstream>& stream) {
        stream.emplace(file, std::ios_base::in | std::ios_base::binary);
        if (!stream->good()) {
            LOG_AND_THROW_EXCEPTION("Failed to open file: " + file.string());
        }
    }

    void close() {
        if (snapshot_istrm_) snapshot_istrm_->close();
        if (compacted_istrm_) compacted_istrm_->close();
    }

    void validate_and_read_stream(std::optional<boost::filesystem::ifstream>& stream, const std::string& stream_name, std::optional<log_entry>& log_entry,
                                  std::string& previous_key_sid) {
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
                    stream->close();
                    stream = std::nullopt;     // If reading fails, reset log_entry
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

    // Next function to read from the appropriate stream
    bool next() {
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
        return true;
    }

    [[nodiscard]] storage_id_type storage() const noexcept {
        return log_entry_.storage();
    }

    void  key(std::string& buf) const noexcept {
        log_entry_.key(buf);
    }

    void  value(std::string& buf) const noexcept {
        log_entry_.value(buf);
    }

    [[nodiscard]] log_entry::entry_type type() const {
        return log_entry_.type();
    }

};

cursor::cursor(const boost::filesystem::path& snapshot_file)
    : log_entry_tracker_(std::make_unique<snapshot_tracker>(snapshot_file)) {}

cursor::cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file)
    :  log_entry_tracker_(std::make_unique<snapshot_tracker>(snapshot_file, compacted_file)) {}


cursor::~cursor() noexcept {
    // TODO: handle close failure
    log_entry_tracker_->close(); 
}

bool cursor::next() {
    // Keep calling next() until we find a non-remove_entry, or no more entries
    while (log_entry_tracker_->next()) {
        if (log_entry_tracker_->type() != log_entry::entry_type::remove_entry) {
            return true;  
        }
    }
    return false;  
}

storage_id_type cursor::storage() const noexcept {
    return log_entry_tracker_->storage();
}

void cursor::key(std::string& buf) const noexcept {
    log_entry_tracker_->key(buf);
}

void cursor::value(std::string& buf) const noexcept {
    log_entry_tracker_->value(buf);
}

std::vector<large_object_view>& cursor::large_objects() noexcept {
    return large_objects_;
}

} // namespace limestone::api
