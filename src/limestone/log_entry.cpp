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
#include "log_entry.h"

#include <array>
#include <cstdint>
#include <limits>
#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::api {

namespace format = limestone::internal::log_entry_format;
using limestone::internal::log_entry_view;

namespace {

// the maximum value of a 32-bit length field
constexpr std::size_t max_length_field_value = std::numeric_limits<std::uint32_t>::max();

/**
 * @brief Appends the value as 1 byte to dest.
 * @param dest the destination
 * @param value the value to append
 */
void append_uint8(std::string& dest, std::uint8_t value) {
    dest.push_back(static_cast<char>(value));
}

/**
 * @brief Appends the value as 4 little-endian bytes to dest.
 * @param dest the destination
 * @param value the value to append
 */
void append_uint32le(std::string& dest, std::uint32_t value) {
    const std::uint32_t buf = htole32(value);
    dest.append(reinterpret_cast<const char*>(&buf), sizeof(buf));  // NOLINT(*-reinterpret-cast)
}

/**
 * @brief Appends the value as 8 little-endian bytes to dest.
 * @param dest the destination
 * @param value the value to append
 */
void append_uint64le(std::string& dest, std::uint64_t value) {
    const std::uint64_t buf = htole64(value);
    dest.append(reinterpret_cast<const char*>(&buf), sizeof(buf));  // NOLINT(*-reinterpret-cast)
}

/**
 * @brief Appends the write_version to dest as the epoch and the minor version, each as 8 little-endian bytes.
 * @param dest the destination
 * @param write_version the write_version to append
 */
void append_write_version(std::string& dest, write_version_type write_version) {
    append_uint64le(dest, static_cast<std::uint64_t>(write_version.get_major()));
    append_uint64le(dest, write_version.get_minor());
}

/**
 * @brief Builds the layout of normal_entry (for normal_with_blob, up to before the number-of-BLOBs field). Does not
 *        check the lengths.
 * @param type normal_entry or normal_with_blob
 * @param storage_id storage_id
 * @param key key
 * @param value value
 * @param write_version write_version
 * @param extra_capacity the number of bytes to be appended later, added to the capacity to reserve
 * @return the bytes
 */
std::string normal_entry_bytes(log_entry::entry_type type, storage_id_type storage_id, std::string_view key,
                               std::string_view value, write_version_type write_version, std::size_t extra_capacity) {
    std::string buffer{};
    buffer.reserve(format::normal_entry_fixed_size + key.size() + value.size() + extra_capacity);
    append_uint8(buffer, static_cast<std::uint8_t>(type));
    append_uint32le(buffer, static_cast<std::uint32_t>(key.size()));
    append_uint32le(buffer, static_cast<std::uint32_t>(value.size()));
    append_uint64le(buffer, static_cast<std::uint64_t>(storage_id));
    buffer.append(key);
    append_write_version(buffer, write_version);
    buffer.append(value);
    return buffer;
}

/**
 * @brief Builds the bytes of a storage entry.
 * @param type a storage entry type
 * @param storage_id storage_id
 * @param write_version write_version
 * @return the bytes
 */
std::string storage_entry_bytes(log_entry::entry_type type, storage_id_type storage_id,
                                write_version_type write_version) {
    std::string buffer{};
    buffer.reserve(format::storage_entry_size);
    append_uint8(buffer, static_cast<std::uint8_t>(type));
    append_uint64le(buffer, static_cast<std::uint64_t>(storage_id));
    append_write_version(buffer, write_version);
    return buffer;
}

/**
 * @brief Builds the bytes of a marker (for marker_end, up to before the CRC type).
 * @param type a marker type
 * @param epoch epoch
 * @return the bytes
 */
std::string marker_bytes(log_entry::entry_type type, epoch_id_type epoch) {
    std::string buffer{};
    buffer.reserve(format::marker_end_size);
    append_uint8(buffer, static_cast<std::uint8_t>(type));
    append_uint64le(buffer, static_cast<std::uint64_t>(epoch));
    return buffer;
}

}  // namespace

log_entry::log_entry(const log_entry_view& view) : entry_(view.serialized_entry()) {
    rebuild_view();
}

log_entry::log_entry(std::string entry) : entry_(std::move(entry)) {
    rebuild_view();
}

log_entry::log_entry(const log_entry& other) : entry_(other.entry_), view_(other.view_.relocated(entry_)) {
    // a copied view_ would still refer to other.entry_, so relocate it to this entry_ (the content is the same, so it
    // is not parsed again)
}

log_entry& log_entry::operator=(const log_entry& other) {
    if (this != &other) {
        entry_ = other.entry_;
        view_ = other.view_.relocated(entry_);
    }
    return *this;
}

log_entry::log_entry(log_entry&& other) noexcept
    : entry_(std::exchange(other.entry_, {})), view_(other.view_.relocated(entry_)) {
    // a short byte sequence is stored inside std::string (SSO) and moves to a new location, so relocate view_ to
    // this entry_
    other.view_ = log_entry_view{};
}

log_entry& log_entry::operator=(log_entry&& other) noexcept {
    if (this != &other) {
        entry_ = std::exchange(other.entry_, {});
        view_ = other.view_.relocated(entry_);
        other.view_ = log_entry_view{};
    }
    return *this;
}

log_entry_view log_entry::view() const noexcept {
    return view_;
}

log_entry log_entry::make_normal_entry(storage_id_type storage_id, std::string_view key, std::string_view value,
                                       write_version_type write_version) {
    if (key.size() > max_length_field_value) {
        LOG_LP(FATAL) << "key length " << key.size() << " exceeds the maximum " << max_length_field_value;
    }
    if (value.size() > max_length_field_value) {
        LOG_LP(FATAL) << "value length " << value.size() << " exceeds the maximum " << max_length_field_value;
    }
    return log_entry{normal_entry_bytes(entry_type::normal_entry, storage_id, key, value, write_version, 0)};
}

log_entry log_entry::make_normal_with_blob(storage_id_type storage_id, std::string_view key, std::string_view value,
                                           write_version_type write_version,
                                           const std::vector<blob_id_type>& blob_ids) {
    if (key.size() > max_length_field_value) {
        LOG_LP(FATAL) << "key length " << key.size() << " exceeds the maximum " << max_length_field_value;
    }
    if (value.size() > max_length_field_value) {
        LOG_LP(FATAL) << "value length " << value.size() << " exceeds the maximum " << max_length_field_value;
    }
    if (blob_ids.size() > max_length_field_value) {
        LOG_LP(FATAL) << "number of BLOBs " << blob_ids.size() << " exceeds the maximum " << max_length_field_value;
    }
    std::string buffer = normal_entry_bytes(entry_type::normal_with_blob, storage_id, key, value, write_version,
                                            format::blob_count_size + blob_ids.size() * format::blob_id_size);
    append_uint32le(buffer, static_cast<std::uint32_t>(blob_ids.size()));
    for (const auto blob_id : blob_ids) {
        append_uint64le(buffer, static_cast<std::uint64_t>(blob_id));
    }
    return log_entry{std::move(buffer)};
}

log_entry log_entry::make_remove_entry(storage_id_type storage_id, std::string_view key,
                                       write_version_type write_version) {
    if (key.size() > max_length_field_value) {
        LOG_LP(FATAL) << "key length " << key.size() << " exceeds the maximum " << max_length_field_value;
    }
    std::string buffer{};
    buffer.reserve(format::remove_entry_fixed_size + key.size());
    append_uint8(buffer, static_cast<std::uint8_t>(entry_type::remove_entry));
    append_uint32le(buffer, static_cast<std::uint32_t>(key.size()));
    append_uint64le(buffer, static_cast<std::uint64_t>(storage_id));
    buffer.append(key);
    append_write_version(buffer, write_version);
    return log_entry{std::move(buffer)};
}

log_entry log_entry::make_clear_storage(storage_id_type storage_id, write_version_type write_version) {
    return log_entry{storage_entry_bytes(entry_type::clear_storage, storage_id, write_version)};
}

log_entry log_entry::make_add_storage(storage_id_type storage_id, write_version_type write_version) {
    return log_entry{storage_entry_bytes(entry_type::add_storage, storage_id, write_version)};
}

log_entry log_entry::make_remove_storage(storage_id_type storage_id, write_version_type write_version) {
    return log_entry{storage_entry_bytes(entry_type::remove_storage, storage_id, write_version)};
}

log_entry log_entry::make_marker_begin(epoch_id_type epoch) {
    return log_entry{marker_bytes(entry_type::marker_begin, epoch)};
}

log_entry log_entry::make_marker_end(epoch_id_type epoch) {
    std::string buffer = marker_bytes(entry_type::marker_end, epoch);
    append_uint8(buffer, static_cast<std::uint8_t>(crc_type::no_crc));
    return log_entry{std::move(buffer)};
}

log_entry log_entry::make_marker_durable(epoch_id_type epoch) {
    return log_entry{marker_bytes(entry_type::marker_durable, epoch)};
}

log_entry log_entry::make_marker_invalidated_begin(epoch_id_type epoch) {
    return log_entry{marker_bytes(entry_type::marker_invalidated_begin, epoch)};
}

void log_entry::rebuild_view() {
    if (entry_.empty()) {
        view_ = log_entry_view{};
        return;
    }
    auto result = log_entry_view::parse(entry_);
    if (! result) {
        LOG_LP(FATAL) << "malformed entry: " << result.result();
    }
    if (result.consumed() != entry_.size()) {
        LOG_LP(FATAL) << "malformed entry: entry size " << result.consumed() << " differs from buffer size " << entry_.size();
    }
    view_ = result.view();
}

void log_entry::write_serialized(FILE* strm, const log_entry_view& entry, write_version_handling handling) {
    const std::string_view bytes = entry.serialized_entry();
    switch (handling) {
    case write_version_handling::keep:
        write_bytes(strm, bytes.data(), bytes.size());
        break;
    case write_version_handling::oldest: {
        // write_version is at the beginning of value_etc; split the entry there and write the oldest version in between
        const auto write_version_offset = static_cast<std::size_t>(entry.serialized_value_etc().data() - bytes.data());
        const write_version_type oldest = format::oldest_write_version();
        const std::array<std::uint64_t, 2> oldest_bytes{htole64(static_cast<std::uint64_t>(oldest.get_major())),
                                                        htole64(oldest.get_minor())};
        static_assert(sizeof(oldest_bytes) == format::write_version_size);
        const std::string_view rest = bytes.substr(write_version_offset + format::write_version_size);
        write_bytes(strm, bytes.data(), write_version_offset);
        write_bytes(strm, oldest_bytes.data(), sizeof(oldest_bytes));
        write_bytes(strm, rest.data(), rest.size());
        break;
    }
    }
}

storage_id_type log_entry::storage() const {
    return view_.storage();
}

log_entry::entry_type log_entry::type() const noexcept {
    return view_.type();
}

epoch_id_type log_entry::epoch_id() const {
    return view_.epoch_id();
}

std::string_view log_entry::serialized_entry() const {
    return view_.serialized_entry();
}

std::string_view log_entry::serialized_key_sid() const {
    return view_.serialized_key_sid();
}

std::string_view log_entry::serialized_value_etc() const {
    return view_.serialized_value_etc();
}

std::string_view log_entry::serialized_blob_ids() const {
    return view_.serialized_blob_ids();
}

std::string_view log_entry::key() const {
    return view_.key();
}

std::string_view log_entry::value() const {
    return view_.value();
}

write_version_type log_entry::write_version() const {
    return view_.write_version();
}

std::vector<blob_id_type> log_entry::blob_ids() const {
    return view_.blob_ids();
}

}  // namespace limestone::api
