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
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace limestone::testing::log_entry_bytes {

// The functions below build entries by hand, following the format description (log_entry_format.h).
// They build test inputs and expected values independently of the code under test. Do not replace them with the
// serialization functions of the code under test (such as the factory): a bug in that code would then also be in
// the expected values, and the tests could not detect it.

/**
 * @brief Returns the value as 1 byte.
 * @param v the value
 * @return the byte sequence
 */
inline std::string u8(std::uint8_t v) {
    return std::string(1, static_cast<char>(v));
}

/**
 * @brief Returns the value as 4 little-endian bytes.
 * @param v the value
 * @return the byte sequence
 */
inline std::string u32le(std::uint32_t v) {
    std::string s{};
    for (int i = 0; i < 4; ++i) {
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xffU));
    }
    return s;
}

/**
 * @brief Returns the value as 8 little-endian bytes.
 * @param v the value
 * @return the byte sequence
 */
inline std::string u64le(std::uint64_t v) {
    std::string s{};
    for (int i = 0; i < 8; ++i) {
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xffU));
    }
    return s;
}

/**
 * @brief Builds the bytes of a normal_entry.
 * @param storage storage_id
 * @param key key
 * @param value value
 * @param epoch the epoch of the write_version
 * @param minor the minor version of the write_version
 * @return the bytes of the entry
 */
inline std::string normal_entry(std::uint64_t storage, std::string_view key, std::string_view value,
                                std::uint64_t epoch, std::uint64_t minor) {
    return u8(1) + u32le(key.size()) + u32le(value.size()) + u64le(storage) + std::string(key)
        + u64le(epoch) + u64le(minor) + std::string(value);
}

/**
 * @brief Builds the bytes of a normal_with_blob.
 * @param storage storage_id
 * @param key key
 * @param value value
 * @param epoch the epoch of the write_version
 * @param minor the minor version of the write_version
 * @param blob_ids the blob_ids
 * @return the bytes of the entry
 */
inline std::string normal_with_blob(std::uint64_t storage, std::string_view key, std::string_view value,
                                    std::uint64_t epoch, std::uint64_t minor, const std::vector<std::uint64_t>& blob_ids) {
    std::string s = u8(10) + u32le(key.size()) + u32le(value.size()) + u64le(storage) + std::string(key)
        + u64le(epoch) + u64le(minor) + std::string(value) + u32le(blob_ids.size());
    for (auto id : blob_ids) {
        s += u64le(id);
    }
    return s;
}

/**
 * @brief Builds the bytes of a remove_entry.
 * @param storage storage_id
 * @param key key
 * @param epoch the epoch of the write_version
 * @param minor the minor version of the write_version
 * @return the bytes of the entry
 */
inline std::string remove_entry(std::uint64_t storage, std::string_view key, std::uint64_t epoch, std::uint64_t minor) {
    return u8(5) + u32le(key.size()) + u64le(storage) + std::string(key) + u64le(epoch) + u64le(minor);
}

/**
 * @brief Builds the bytes of a storage entry.
 * @param type the entry type value
 * @param storage storage_id
 * @param epoch the epoch of the write_version
 * @param minor the minor version of the write_version
 * @return the bytes of the entry
 */
inline std::string storage_entry(std::uint8_t type, std::uint64_t storage, std::uint64_t epoch, std::uint64_t minor) {
    return u8(type) + u64le(storage) + u64le(epoch) + u64le(minor);
}

/**
 * @brief Builds the bytes of a marker. The CRC type of marker_end is not included.
 * @param type the entry type value
 * @param epoch the epoch
 * @return the bytes of the entry
 */
inline std::string marker(std::uint8_t type, std::uint64_t epoch) {
    return u8(type) + u64le(epoch);
}

}  // namespace limestone::testing::log_entry_bytes
