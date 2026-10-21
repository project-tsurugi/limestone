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
#include "log_entry/log_entry_view.h"

#include <cstring>
#include <endian.h>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal {

namespace format = log_entry_format;

namespace {

// reads the first 4 bytes of src; src must be at least 4 bytes long
std::uint32_t load_uint32le(std::string_view src) noexcept {
    std::uint32_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le32toh(buf);
}

// reads the first 8 bytes of src; src must be at least 8 bytes long
std::uint64_t load_uint64le(std::string_view src) noexcept {
    std::uint64_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le64toh(buf);
}

log_entry_parse_result short_entry_result(std::size_t required) noexcept {
    log_entry_parse_result r{};
    r.result = log_entry_parse_result::status::short_entry;
    r.required = required;
    return r;
}

}  // namespace

log_entry_view::log_entry_view(std::string_view entry, entry_type type, std::uint32_t key_length,
                               std::uint32_t value_length, std::uint32_t blob_count) noexcept
    : entry_(entry), type_(type), key_length_(key_length), value_length_(value_length), blob_count_(blob_count) {}

log_entry_parse_result log_entry_view::parse(std::string_view input) noexcept {
    using status = log_entry_parse_result::status;

    if (input.empty()) {
        return short_entry_result(format::entry_type_size);
    }
    const auto type_value = static_cast<std::uint8_t>(input[0]);
    if (! format::is_valid_entry_type(type_value)) {
        log_entry_parse_result r{};
        r.result = status::invalid_type;
        r.type_value = type_value;
        return r;
    }
    const auto type = static_cast<entry_type>(type_value);

    std::uint32_t key_length = 0;
    std::uint32_t value_length = 0;
    std::uint32_t blob_count = 0;
    std::size_t length = 0;  // the size of the whole entry
    switch (type) {
    case entry_type::normal_entry:
    case entry_type::normal_with_blob: {
        constexpr std::size_t header_size = format::entry_type_size + format::key_length_size + format::value_length_size;
        if (input.size() < header_size) {
            return short_entry_result(header_size);
        }
        key_length = load_uint32le(input.substr(format::entry_type_size));
        value_length = load_uint32le(input.substr(format::entry_type_size + format::key_length_size));
        length = format::normal_entry_fixed_size + key_length + value_length;
        if (type == entry_type::normal_with_blob) {
            length += format::blob_count_size;
            if (input.size() < length) {
                return short_entry_result(length);
            }
            blob_count = load_uint32le(input.substr(length - format::blob_count_size));
            length += static_cast<std::size_t>(blob_count) * format::blob_id_size;
        }
        break;
    }
    case entry_type::remove_entry: {
        constexpr std::size_t header_size = format::entry_type_size + format::key_length_size;
        if (input.size() < header_size) {
            return short_entry_result(header_size);
        }
        key_length = load_uint32le(input.substr(format::entry_type_size));
        length = format::remove_entry_fixed_size + key_length;
        break;
    }
    case entry_type::clear_storage:
    case entry_type::add_storage:
    case entry_type::remove_storage:
        length = format::storage_entry_size;
        break;
    case entry_type::marker_end:
        length = format::marker_end_size;
        break;
    default:  // marker_begin、marker_durable、marker_invalidated_begin
        length = format::marker_size;
        break;
    }
    if (input.size() < length) {
        return short_entry_result(length);
    }

    log_entry_parse_result r{};
    r.result = status::ok;
    r.view = log_entry_view{input.substr(0, length), type, key_length, value_length, blob_count};
    r.consumed = length;
    return r;
}

log_entry_view::entry_type log_entry_view::type() const noexcept {
    return type_;
}

std::string_view log_entry_view::serialized_entry() const {
    if (type_ == entry_type::this_id_is_not_used) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_;
}

std::string_view log_entry_view::serialized_key_sid() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_.substr(key_sid_offset(), format::storage_id_size + key_length_);
}

std::string_view log_entry_view::serialized_value_etc() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_.substr(value_etc_offset(), format::write_version_size + value_length_);
}

std::string_view log_entry_view::serialized_blob_ids() const {
    if (type_ != entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_.substr(value_etc_offset() + format::write_version_size + value_length_ + format::blob_count_size);
}

log_entry_view::storage_id_type log_entry_view::storage() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return static_cast<storage_id_type>(load_uint64le(entry_.substr(key_sid_offset())));
}

std::string_view log_entry_view::key() const {
    if (! format::is_data_entry(type_)) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_.substr(key_sid_offset() + format::storage_id_size, key_length_);
}

std::string_view log_entry_view::value() const {
    if (type_ != entry_type::normal_entry && type_ != entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return entry_.substr(value_etc_offset() + format::write_version_size, value_length_);
}

log_entry_view::write_version_type log_entry_view::write_version() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    const std::string_view version = entry_.substr(value_etc_offset());
    return {static_cast<epoch_id_type>(load_uint64le(version)),
            load_uint64le(version.substr(format::write_version_epoch_size))};
}

std::vector<log_entry_view::blob_id_type> log_entry_view::blob_ids() const {
    const std::string_view ids = serialized_blob_ids();
    std::vector<blob_id_type> result{};
    result.reserve(blob_count_);
    for (std::size_t i = 0; i < blob_count_; ++i) {
        result.emplace_back(static_cast<blob_id_type>(load_uint64le(ids.substr(i * format::blob_id_size))));
    }
    return result;
}

log_entry_view::epoch_id_type log_entry_view::epoch_id() const {
    if (! format::is_marker(type_)) {
        LOG_LP(FATAL) << "not valid for entry type " << static_cast<int>(type_);
    }
    return static_cast<epoch_id_type>(load_uint64le(entry_.substr(format::entry_type_size)));
}

bool log_entry_view::is_data_or_storage() const noexcept {
    return format::is_data_entry(type_) || format::is_storage_entry(type_);
}

std::size_t log_entry_view::key_sid_offset() const noexcept {
    if (type_ == entry_type::normal_entry || type_ == entry_type::normal_with_blob) {
        return format::entry_type_size + format::key_length_size + format::value_length_size;
    }
    if (type_ == entry_type::remove_entry) {
        return format::entry_type_size + format::key_length_size;
    }
    return format::entry_type_size;
}

std::size_t log_entry_view::value_etc_offset() const noexcept {
    return key_sid_offset() + format::storage_id_size + key_length_;
}

log_entry_parse_result::operator bool() const noexcept {
    return result == status::ok;
}

}  // namespace limestone::internal
