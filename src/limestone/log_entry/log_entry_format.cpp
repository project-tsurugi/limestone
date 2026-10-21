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
#include "log_entry/log_entry_format.h"

namespace limestone::internal::log_entry_format {

std::string_view to_string_view(entry_type type) noexcept {
    switch (type) {
        case entry_type::this_id_is_not_used: return "this_id_is_not_used";
        case entry_type::normal_entry: return "normal_entry";
        case entry_type::normal_with_blob: return "normal_with_blob";
        case entry_type::remove_entry: return "remove_entry";
        case entry_type::marker_begin: return "marker_begin";
        case entry_type::marker_end: return "marker_end";
        case entry_type::marker_durable: return "marker_durable";
        case entry_type::marker_invalidated_begin: return "marker_invalidated_begin";
        case entry_type::clear_storage: return "clear_storage";
        case entry_type::add_storage: return "add_storage";
        case entry_type::remove_storage: return "remove_storage";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& out, entry_type type) {
    return out << to_string_view(type);
}

bool is_data_entry(entry_type type) noexcept {
    return type == entry_type::normal_entry || type == entry_type::normal_with_blob || type == entry_type::remove_entry;
}

bool is_storage_entry(entry_type type) noexcept {
    return type == entry_type::clear_storage || type == entry_type::add_storage || type == entry_type::remove_storage;
}

bool is_marker(entry_type type) noexcept {
    return type == entry_type::marker_begin || type == entry_type::marker_end || type == entry_type::marker_durable
        || type == entry_type::marker_invalidated_begin;
}

bool is_valid_entry_type(std::uint8_t value) noexcept {
    const auto type = static_cast<entry_type>(value);
    return is_data_entry(type) || is_storage_entry(type) || is_marker(type);
}

int compare_key_sid(std::string_view a, std::string_view b) noexcept {
    return a.compare(b);
}

}  // namespace limestone::internal::log_entry_format
