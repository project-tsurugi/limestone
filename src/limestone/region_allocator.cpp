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
#include "region_allocator.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "limestone_exception_helper.h"
#include "logging_helper.h"
#include "log_entry/log_entry_format.h"

namespace limestone::internal {

region_allocator::region_allocator(locator snapshot_locator, std::optional<locator> compacted_locator,
                                   std::size_t region_records)
    : snapshot_locator_(std::move(snapshot_locator)), compacted_locator_(std::move(compacted_locator)),
      region_records_(region_records) {
    if (region_records_ == 0) {
        LOG_LP(FATAL) << "region records is 0";
    }
    // the file with more records is the primary; the compacted WAL file on a tie
    if (compacted_locator_.has_value() && compacted_locator_->record_count() >= snapshot_locator_.record_count()) {
        primary_is_snapshot_ = false;
        primary_ = &*compacted_locator_;
        secondary_ = &snapshot_locator_;
    } else {
        primary_is_snapshot_ = true;
        primary_ = &snapshot_locator_;
        secondary_ = compacted_locator_.has_value() ? &*compacted_locator_ : nullptr;
    }
    region_count_ = (primary_->record_count() + region_records_ - 1) / region_records_;
}

std::optional<region_range> region_allocator::allocate(log_entry_reader& snapshot_reader,
                                                       log_entry_reader* compacted_reader) {
    if (compacted_locator_.has_value() && compacted_reader == nullptr) {
        LOG_LP(FATAL) << "no reader for the compacted file: " << compacted_locator_->file();
    }
    const std::size_t j = next_region_.fetch_add(1);
    if (j >= region_count_) {
        return std::nullopt;
    }
    if (primary_is_snapshot_) {
        return compute_range(j, snapshot_reader, compacted_reader);
    }
    return compute_range(j, *compacted_reader, &snapshot_reader);
}

std::size_t region_allocator::region_count() const noexcept {
    return region_count_;
}

region_range region_allocator::compute_range(std::size_t j, log_entry_reader& primary_reader,
                                             log_entry_reader* secondary_reader) const {
    const locator& primary = *primary_;
    const std::size_t first_record = j * region_records_;
    const std::size_t next_record = first_record + region_records_;
    const bool is_last = next_record >= primary.record_count();

    std::uint64_t primary_start = primary.position(first_record);
    std::uint64_t primary_end = is_last ? primary.locatee_size() : primary.position(next_record);
    std::optional<std::string> start_key{};
    if (j != 0) {
        start_key = probe(primary_reader, primary_start, primary.locatee_size());
    }
    std::optional<std::string> end_key{};
    if (! is_last) {
        end_key = probe(primary_reader, primary_end, primary.locatee_size());
    }

    std::uint64_t secondary_start = 0;
    std::uint64_t secondary_end = 0;
    if (secondary_ != nullptr) {
        const locator& secondary = *secondary_;
        if (start_key.has_value()) {
            // the last record whose key is below the start key = the one before the first record whose key is at or
            // above the start key
            const std::size_t k = lower_bound(*secondary_reader, *start_key);
            secondary_start = k == 0 ? 0 : secondary.position(k - 1);
        }
        if (end_key.has_value()) {
            const std::size_t k = lower_bound(*secondary_reader, *end_key);
            secondary_end = k == secondary.record_count() ? secondary.locatee_size() : secondary.position(k);
        } else {
            secondary_end = secondary.locatee_size();
        }
    }

    region_range range{};
    if (primary_is_snapshot_) {
        range.snapshot_start = primary_start;
        range.snapshot_end = primary_end;
        range.compacted_start = secondary_start;
        range.compacted_end = secondary_end;
    } else {
        range.snapshot_start = secondary_start;
        range.snapshot_end = secondary_end;
        range.compacted_start = primary_start;
        range.compacted_end = primary_end;
    }
    range.start_key = std::move(start_key);
    range.end_key = std::move(end_key);
    return range;
}

std::size_t region_allocator::lower_bound(log_entry_reader& secondary_reader, const std::string& key) const {
    const locator& secondary = *secondary_;
    std::size_t low = 0;
    std::size_t high = secondary.record_count();
    while (low < high) {
        const std::size_t mid = low + (high - low) / 2;
        const std::string mid_key = probe(secondary_reader, secondary.position(mid), secondary.locatee_size());
        if (log_entry_format::compare_key_sid(mid_key, key) < 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

std::string region_allocator::probe(log_entry_reader& reader, std::uint64_t offset, std::uint64_t file_size) {
    if (offset >= file_size) {
        LOG_AND_THROW_EXCEPTION("the locator record " + std::to_string(offset) + " is not within the file size "
                                + std::to_string(file_size) + ": " + reader.file().string());
    }
    std::uint64_t end_offset = std::min(offset + probe_size, file_size);
    while (true) {
        reader.set_range(offset, end_offset);
        const log_entry_read_result read_result = reader.next();
        switch (read_result.result()) {
        case log_entry_read_result::status::ok: {
            const log_entry_view view = read_result.view();
            if (! log_entry_format::is_data_entry(view.type())) {
                LOG_AND_THROW_EXCEPTION("the entry at " + std::to_string(offset) + " is not a data entry: "
                                        + reader.file().string());
            }
            return std::string(view.serialized_key_sid());
        }
        case log_entry_read_result::status::end:
            // the range is not empty, so the file is smaller than the size recorded in the locator
            LOG_AND_THROW_EXCEPTION("the file is shorter than the locator says at " + std::to_string(offset) + ": "
                                    + reader.file().string());
        case log_entry_read_result::status::error:
            break;
        }
        if (read_result.error().value() != limestone::api::log_entry::read_error::short_entry) {
            LOG_AND_THROW_EXCEPTION("the entry at " + std::to_string(offset) + " is broken: " + reader.file().string()
                                    + ": " + read_result.error().message());
        }
        // widen the end position to what the entry needs. If it does not go beyond what has been read (the read ended
        // short of the range), the file is smaller than the size recorded in the locator
        const std::uint64_t required_end = offset + read_result.required();
        if (required_end > file_size || required_end <= end_offset) {
            LOG_AND_THROW_EXCEPTION("the entry at " + std::to_string(offset) + " is cut by the file size "
                                    + std::to_string(file_size) + ": " + reader.file().string());
        }
        end_offset = required_end;
    }
}

}  // namespace limestone::internal
