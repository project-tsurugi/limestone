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
#include <map>
#include "cursor_impl.h"
#include <glog/logging.h>
#include "limestone_exception_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::api::chunk_offset_t;

std::unique_ptr<cursor> cursor_impl::create_cursor(const boost::filesystem::path& snapshot_file,
                                                    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) {
    auto cursor_instance = std::unique_ptr<cursor>(new cursor(snapshot_file)); 
    cursor_instance->pimpl->set_clear_storage(clear_storage); 
    return cursor_instance; 
}

std::unique_ptr<cursor> cursor_impl::create_cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                                                    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) {
    auto cursor_instance = std::unique_ptr<cursor>(new cursor(snapshot_file, compacted_file));
    cursor_instance->pimpl->set_clear_storage(clear_storage);
    return cursor_instance;
}

std::pair<std::unique_ptr<cursor>, chunk_offset_t> cursor_impl::create_chunk_cursor(
    const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
    const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage, chunk_offset_t offset) {
    auto cursor_instance = std::unique_ptr<cursor>(new cursor(snapshot_file, compacted_file));
    cursor_instance->pimpl->set_clear_storage(clear_storage);
    auto next_offset = cursor_instance->pimpl->verify_chunk_mark(offset);
    return std::make_pair(std::move(cursor_instance), next_offset);
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file) 
    : compacted_istrm_(std::nullopt) {
    open(snapshot_file, snapshot_istrm_);
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file) {
    open(snapshot_file, snapshot_istrm_);
    open(compacted_file, compacted_istrm_); 
}

cursor_impl::cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file, chunk_offset_t offset)
    : offset_(offset) {
    open(snapshot_file, snapshot_istrm_);
    if (offset.c_off >= 0 && boost::filesystem::exists(compacted_file)) {
        open(compacted_file, compacted_istrm_);
    } else {
        compacted_istrm_ = std::nullopt;
    }
    chunked_read_ = true;
}

void cursor_impl::open(const boost::filesystem::path& file, std::optional<boost::filesystem::ifstream>& stream) {
    stream.emplace(file, std::ios_base::in | std::ios_base::binary);
    if (!stream->is_open() || !stream->good()) {
        LOG_AND_THROW_EXCEPTION("Failed to open file: " + file.string());
    }
}

static std::pair<long, std::string> verify_chunk_mark_1(boost::filesystem::ifstream& istrm, const std::string& filetype, long marker_pos, const std::string& low) {
    log_entry le;
    istrm.seekg(marker_pos, std::ios::beg);
    if (!le.read(istrm)) {
        throw std::runtime_error(filetype + " read");
    }
    if (le.type() != log_entry::entry_type::marker_chunk) {
        throw std::runtime_error(filetype + " read-chunk");
    }
    auto next_marker = static_cast<long>(le.epoch_id());
    std::pair<long, std::string> ret{ -1, {} };
    if (next_marker >= 0) {
        // seek to next chunk
        istrm.seekg(next_marker, std::ios::beg);
        // read next key
        if (!le.read(istrm)) {
            throw std::runtime_error(filetype + " read-next-chunk");
        }
        if (le.type() != log_entry::entry_type::marker_chunk) {
            throw std::runtime_error(filetype + " read-next-chunk-marker");
        }
        if (!le.read(istrm)) {
            throw std::runtime_error(filetype + " read-next-head-entry");
        }
        // rewind
        istrm.seekg(marker_pos + 9, std::ios::beg);
        ret = std::make_pair(next_marker, le.key_sid());
    }

    // skip to low
    while (istrm) {
        auto prev_pos = istrm.tellg();
        if (!le.read(istrm)) {

        }
        if (le.type() != log_entry::entry_type::normal_entry && le.type() != log_entry::entry_type::normal_with_blob && le.type() != log_entry::entry_type::remove_entry)
            continue;
        if (le.key_sid() >= low) {
            istrm.seekg(prev_pos);
            break;
        }
    }
    return ret;
}

chunk_offset_t cursor_impl::verify_chunk_mark(chunk_offset_t offset) {
    if (offset.c_off < 0) {
        if (offset.ss_off < 0) { throw std::runtime_error("both eof"); }
        auto [ ss_next_off, ss_high ] = verify_chunk_mark_1(*snapshot_istrm_, "ss", offset.ss_off, offset.low);
        high_ = ss_next_off >= 0 ? std::optional{ss_high} : std::nullopt;
        return { ss_next_off, -1, ss_high };
    }
    if (offset.ss_off < 0) {
        auto [ c_next_off, c_high ] = verify_chunk_mark_1(*compacted_istrm_, "compacted", offset.c_off, offset.low);
        high_ = c_next_off >= 0 ? std::optional{c_high} : std::nullopt;
        return { -1, c_next_off, c_high };
    }

    auto [ ss_next_off, ss_high ] = verify_chunk_mark_1(*snapshot_istrm_, "ss", offset.ss_off, offset.low);
    auto [ c_next_off, c_high ] = verify_chunk_mark_1(*compacted_istrm_, "compacted", offset.c_off, offset.low);

    if (ss_next_off == -1 && c_next_off == -1) {  // both last chunk
        return { -1, -1, {} };
    }
    if (c_next_off == -1 || ss_high < c_high) {
        // next: advance ss chunk
        high_ = ss_high;
        return { ss_next_off, offset.c_off, ss_high };
    } else {
        // next: advance c chunk
        high_ = c_high;
        return { offset.ss_off, c_next_off, c_high };
    }
}

void cursor_impl::close() {
    if (snapshot_istrm_) snapshot_istrm_->close();
    if (compacted_istrm_) compacted_istrm_->close();
}

void cursor_impl::validate_and_read_stream(std::optional<boost::filesystem::ifstream>& stream, const std::string& stream_name,
                                           std::optional<log_entry>& log_entry, std::string& previous_key_sid) {
    while (stream) {
        // If the stream is not in good condition, close it and exit
        if (!stream->good()) {
            DVLOG_LP(log_trace) << stream_name << " stream is not good, closing it.";
            stream->close();
            stream = std::nullopt;
            return;
        }

        // If the entry is not yet read, read it
        if (!log_entry) {
            log_entry.emplace();  // Construct a new log_entry
            do {
                if (!log_entry->read(*stream) || (chunked_read_ && log_entry->key_sid() >= high_)) {
                    // If reading fails, close the stream and reset the log_entry
                    stream->close();
                    stream = std::nullopt;
                    log_entry = std::nullopt;
                    return;
                }
            } while (log_entry->type() != log_entry::entry_type::normal_entry &&
                     log_entry->type() != log_entry::entry_type::normal_with_blob);
            // Check if the key_sid is in ascending order
            // TODO: Key order violation is detected here and the process is aborted.
            // However, this check should be moved to an earlier point, and if the key order is invalid,
            // a different processing method should be considered instead of aborting immediately.
            if (!previous_key_sid.empty() && log_entry->key_sid() < previous_key_sid) {
                LOG(ERROR) << "Key order violation in " << stream_name << ": current key_sid (" << log_entry->key_sid()
                           << ") is smaller than the previous key_sid (" << previous_key_sid << ")";
                THROW_LIMESTONE_EXCEPTION("Key order violation detected in " + stream_name);
            }
            // Skip processing if key_sid is the same as previous_key_sid
            if (!previous_key_sid.empty() && log_entry->key_sid() == previous_key_sid) {
                DVLOG_LP(log_trace_fine) << stream_name << " log entry key_sid (" << log_entry->key_sid() << ") is same as previous, skipping.";
                log_entry = std::nullopt;
                continue;
            }
            // Update the previous key_sid to the current one
            previous_key_sid = log_entry->key_sid();
        }

        // Check the validity of the entry
        if (is_relevant_entry(log_entry.value())) {
            // If a valid entry is found, return
            return;
        }

        // If the entry is invalid, reset the log_entry and continue processing the stream
        log_entry = std::nullopt;
    }
}

bool cursor_impl::is_relevant_entry(const limestone::api::log_entry& entry) {
        // Step 1: Check if the entry is one of normal_entry, remove_entry, or normal_with_blob
        if (entry.type() != limestone::api::log_entry::entry_type::normal_entry &&
            entry.type() != limestone::api::log_entry::entry_type::remove_entry &&
            entry.type() != limestone::api::log_entry::entry_type::normal_with_blob) {
            return false;  // Skip this entry if it's not a normal, remove, or blob entry
        }


        // Step 2: Get the storage ID from log_entry
        auto storage_id = entry.storage();  // Assuming storage() returns the storage ID
        // Step 3: Check if clear_storage_ contains the same storage ID
        auto it = clear_storage_.find(storage_id);
        if (it != clear_storage_.end()) {
            // Step 4: Retrieve the write_version from log_entry (only if the storage ID is found)
            write_version_type wv;
            entry.write_version(wv);

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
        // Read from the snapshot stream if the snapshot_log_entry_ is empty
        if (!snapshot_log_entry_) {
            validate_and_read_stream(snapshot_istrm_, "Snapshot", snapshot_log_entry_, previous_snapshot_key_sid);
        }

        // Read from the compacted stream if the compacted_log_entry_ is empty
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
        // Check if the current log_entry_ is a normal entry or normal_with_blob
        if (log_entry_.type() == log_entry::entry_type::normal_entry ||
            log_entry_.type() == log_entry::entry_type::normal_with_blob) {
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

std::vector<limestone::api::blob_id_type> cursor_impl::blob_ids() const {
    return log_entry_.get_blob_ids();
}

void cursor_impl::set_clear_storage(const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) {
    clear_storage_ = clear_storage;
}

} // namespace limestone::internal
