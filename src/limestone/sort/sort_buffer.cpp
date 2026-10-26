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
#include "sort/sort_buffer.h"

#include <algorithm>
#include <cstring>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"
#include "log_entry/log_entry_format.h"

namespace limestone::internal {

namespace format = log_entry_format;

sort_buffer::sort_buffer(std::size_t arena_size_bytes) : arena_size_bytes_(arena_size_bytes) {
    if (arena_size_bytes == 0 || arena_size_bytes > max_arena_size_bytes) {
        LOG_LP(FATAL) << "arena size " << arena_size_bytes << " must be between 1 and " << max_arena_size_bytes;
    }
}

sort_buffer::append_result sort_buffer::append(const log_entry_view& entry) {
    if (! format::is_data_entry(entry.type())) {
        LOG_LP(FATAL) << "not valid for entry type " << entry.type();
    }
    const std::string_view bytes = entry.serialized_entry();
    if (bytes.size() > arena_size_bytes_) {
        return append_result::oversized;
    }
    if (arena_size_bytes_ - used_ < bytes.size()) {
        return append_result::full;
    }
    if (arena_.empty()) {
        arena_.assign(arena_size_bytes_, '\0');
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::memcpy(arena_.data() + used_, bytes.data(), bytes.size());
    index_.push_back(static_cast<std::uint32_t>(used_));
    used_ += bytes.size();
    return append_result::appended;
}

std::size_t sort_buffer::arena_size_bytes() const noexcept {
    return arena_size_bytes_;
}

std::size_t sort_buffer::entry_count() const noexcept {
    return index_.size();
}

bool sort_buffer::empty() const noexcept {
    return index_.empty();
}

log_entry_view sort_buffer::entry_at(std::size_t i) const {
    if (i >= index_.size()) {
        LOG_LP(FATAL) << "index " << i << " is out of range (entry count " << index_.size() << ")";
    }
    // parsing always succeeds because the whole entry has been put into the arena
    return log_entry_view::parse(std::string_view(arena_).substr(index_[i])).view();
}

void sort_buffer::sort() {
    const std::string_view arena{arena_};
    // the comparison reads key_sid and write_version directly from the arena, not through the accessors of the view
    const auto less = [arena](std::uint32_t a, std::uint32_t b) noexcept {
        // from the beginning of the entry to the end of the arena; the format functions take the keys out of it by
        // reading the entry type and the key length
        const std::string_view entry_a = arena.substr(a);
        const std::string_view entry_b = arena.substr(b);
        const int c = format::compare_key_sid(format::data_entry_key_sid(entry_a), format::data_entry_key_sid(entry_b));
        if (c != 0) {
            return c < 0;
        }
        // within the same key_sid, write_version in descending order
        return format::data_entry_write_version(entry_b) < format::data_entry_write_version(entry_a);
    };
    std::sort(index_.begin(), index_.end(), less);
}

void sort_buffer::clear() noexcept {
    index_.clear();
    used_ = 0;
}

std::string_view to_string_view(sort_buffer::append_result result) noexcept {
    using append_result = sort_buffer::append_result;
    switch (result) {
    case append_result::appended: return "appended";
    case append_result::full: return "full";
    case append_result::oversized: return "oversized";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& out, sort_buffer::append_result result) {
    return out << to_string_view(result);
}

}  // namespace limestone::internal
