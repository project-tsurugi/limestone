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

/**
 * @brief Reads the first 4 bytes of src as a little-endian value.
 * @param src the source. It must be at least 4 bytes long.
 * @return the value read
 */
std::uint32_t load_uint32le(std::string_view src) noexcept {
    std::uint32_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le32toh(buf);
}

/**
 * @brief Reads the first 8 bytes of src as a little-endian value.
 * @param src the source. It must be at least 8 bytes long.
 * @return the value read
 */
std::uint64_t load_uint64le(std::string_view src) noexcept {
    std::uint64_t buf{};
    std::memcpy(&buf, src.data(), sizeof(buf));
    return le64toh(buf);
}

}  // namespace

log_entry_view::log_entry_view(std::string_view entry, entry_type type, std::uint32_t key_length,
                               std::uint32_t value_length, std::uint32_t blob_count) noexcept
    : entry_(entry), type_(type), key_length_(key_length), value_length_(value_length), blob_count_(blob_count) {}

log_entry_parse_result log_entry_view::parse(std::string_view input) noexcept {
    if (input.empty()) {
        return log_entry_parse_result::short_entry(format::entry_type_size);
    }
    const auto type_value = static_cast<std::uint8_t>(input[0]);
    if (! format::is_valid_entry_type(type_value)) {
        return log_entry_parse_result::invalid_type(type_value);
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
            return log_entry_parse_result::short_entry(header_size);
        }
        key_length = load_uint32le(input.substr(format::entry_type_size));
        value_length = load_uint32le(input.substr(format::entry_type_size + format::key_length_size));
        length = format::normal_entry_fixed_size + key_length + value_length;
        if (type == entry_type::normal_with_blob) {
            length += format::blob_count_size;
            if (input.size() < length) {
                return log_entry_parse_result::short_entry(length);
            }
            blob_count = load_uint32le(input.substr(length - format::blob_count_size));
            length += static_cast<std::size_t>(blob_count) * format::blob_id_size;
        }
        break;
    }
    case entry_type::remove_entry: {
        constexpr std::size_t header_size = format::entry_type_size + format::key_length_size;
        if (input.size() < header_size) {
            return log_entry_parse_result::short_entry(header_size);
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
    case entry_type::marker_begin:
    case entry_type::marker_durable:
    case entry_type::marker_invalidated_begin:
        length = format::marker_size;
        break;
    case entry_type::marker_end:
        length = format::marker_end_size;
        break;
    case entry_type::this_id_is_not_used:
        // unreachable: is_valid_entry_type() has excluded this value
        LOG_LP(FATAL) << "unexpected entry type " << type;
        break;
    }
    if (input.size() < length) {
        return log_entry_parse_result::short_entry(length);
    }

    return log_entry_parse_result::ok(log_entry_view{input.substr(0, length), type, key_length, value_length, blob_count});
}

log_entry_view::entry_type log_entry_view::type() const noexcept {
    return type_;
}

std::string_view log_entry_view::serialized_entry() const {
    if (type_ == entry_type::this_id_is_not_used) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_;
}

std::string_view log_entry_view::serialized_key_sid() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(key_sid_offset(), format::storage_id_size + key_length_);
}

std::string_view log_entry_view::serialized_value_etc() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(value_etc_offset(), format::write_version_size + value_length_);
}

std::string_view log_entry_view::serialized_blob_ids() const {
    if (type_ != entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(value_etc_offset() + format::write_version_size + value_length_ + format::blob_count_size);
}

log_entry_view::storage_id_type log_entry_view::storage() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return static_cast<storage_id_type>(load_uint64le(entry_.substr(key_sid_offset())));
}

std::string_view log_entry_view::key() const {
    if (! format::is_data_entry(type_)) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(key_sid_offset() + format::storage_id_size, key_length_);
}

std::string_view log_entry_view::value() const {
    if (type_ != entry_type::normal_entry && type_ != entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(value_etc_offset() + format::write_version_size, value_length_);
}

log_entry_view::write_version_type log_entry_view::write_version() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    const std::string_view version = entry_.substr(value_etc_offset());
    return {static_cast<epoch_id_type>(load_uint64le(version)),
            load_uint64le(version.substr(format::write_version_epoch_size))};
}

std::vector<log_entry_view::blob_id_type> log_entry_view::blob_ids() const {
    if (type_ != entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
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
        LOG_LP(FATAL) << "not valid for entry type " << type_;
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

log_entry_parse_result::log_entry_parse_result(status result, const log_entry_view& view, std::size_t required,
                                               std::uint8_t type_value) noexcept
    : result_(result), view_(view), required_(required), type_value_(type_value) {}

log_entry_parse_result log_entry_parse_result::ok(const log_entry_view& view) {
    if (view.type() == log_entry_view::entry_type::this_id_is_not_used) {
        LOG_LP(FATAL) << "default-constructed view";
    }
    return {status::ok, view, 0, 0};
}

log_entry_parse_result log_entry_parse_result::short_entry(std::size_t required) noexcept {
    return {status::short_entry, log_entry_view{}, required, 0};
}

log_entry_parse_result log_entry_parse_result::invalid_type(std::uint8_t type_value) noexcept {
    return {status::invalid_type, log_entry_view{}, 0, type_value};
}

log_entry_parse_result::status log_entry_parse_result::result() const noexcept {
    return result_;
}

log_entry_parse_result::operator bool() const noexcept {
    return result_ == status::ok;
}

log_entry_view log_entry_parse_result::view() const {
    if (result_ != status::ok) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return view_;
}

std::size_t log_entry_parse_result::consumed() const {
    if (result_ != status::ok) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return view_.serialized_entry().size();
}

std::size_t log_entry_parse_result::required() const {
    if (result_ != status::short_entry) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return required_;
}

std::uint8_t log_entry_parse_result::type_value() const {
    if (result_ != status::invalid_type) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return type_value_;
}

std::string_view to_string_view(log_entry_parse_result::status result) noexcept {
    switch (result) {
        case log_entry_parse_result::status::ok: return "ok";
        case log_entry_parse_result::status::short_entry: return "short_entry";
        case log_entry_parse_result::status::invalid_type: return "invalid_type";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& out, log_entry_parse_result::status result) {
    return out << to_string_view(result);
}

}  // namespace limestone::internal
