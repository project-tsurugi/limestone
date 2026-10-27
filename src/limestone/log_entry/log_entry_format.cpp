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

#include <cstring>
#include <endian.h>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

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

write_version_type oldest_write_version() {
    return write_version_type{0, 0};
}

std::uint32_t load_uint32le(std::string_view src) noexcept {
    std::uint32_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le32toh(buf);
}

std::uint64_t load_uint64le(std::string_view src) noexcept {
    std::uint64_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le64toh(buf);
}

std::size_t key_sid_offset(entry_type type) {
    switch (type) {
    case entry_type::normal_entry:
    case entry_type::normal_with_blob:
        return entry_type_size + key_length_size + value_length_size;
    case entry_type::remove_entry:
        return entry_type_size + key_length_size;
    case entry_type::clear_storage:
    case entry_type::add_storage:
    case entry_type::remove_storage:
        return entry_type_size;
    case entry_type::this_id_is_not_used:
    case entry_type::marker_begin:
    case entry_type::marker_end:
    case entry_type::marker_durable:
    case entry_type::marker_invalidated_begin:
        break;
    }
    LOG_LP(FATAL) << "not valid for entry type " << type;
    return 0;
}

namespace {

/// the position and the length of key_sid in an entry
struct key_sid_location {
    std::size_t offset;
    std::size_t length;
};

/**
 * @brief Locates key_sid in the bytes of a complete data entry.
 * @param entry the bytes of a data entry
 * @return the position and the length of key_sid
 */
key_sid_location locate_data_entry_key_sid(std::string_view entry) noexcept {
    const auto type = static_cast<entry_type>(static_cast<std::uint8_t>(entry[0]));
    // all three data entry types have the key length right after the entry type
    const std::uint32_t key_length = load_uint32le(entry.substr(entry_type_size));
    return {key_sid_offset(type), storage_id_size + key_length};
}

}  // namespace

std::string_view data_entry_key_sid(std::string_view entry) noexcept {
    const key_sid_location key_sid = locate_data_entry_key_sid(entry);
    return entry.substr(key_sid.offset, key_sid.length);
}

write_version_type data_entry_write_version(std::string_view entry) noexcept {
    const key_sid_location key_sid = locate_data_entry_key_sid(entry);
    const std::string_view version = entry.substr(key_sid.offset + key_sid.length);
    return {static_cast<epoch_id_type>(load_uint64le(version)),
            load_uint64le(version.substr(write_version_epoch_size))};
}

}  // namespace limestone::internal::log_entry_format
