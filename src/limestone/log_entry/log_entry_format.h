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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string_view>

#include <limestone/api/blob_id_type.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

/**
 * @file log_entry_format.h
 * @brief The format of log entries.
 *
 * The entry format is common to WAL files, snapshot files, compacted WAL files and memory.
 * All numbers are little-endian. The numbers in parentheses are sizes in bytes.
 *
 * | entry type | layout |
 * |---|---|
 * | normal_entry | type (1), key length (4), value length (4), key_sid (8 + key length), value_etc (16 + value length) |
 * | normal_with_blob | the same layout as normal_entry, followed by the number of BLOBs (4) and the blob_ids (8 x number) |
 * | remove_entry | type (1), key length (4), key_sid (8 + key length), write_version (16) |
 * | clear_storage, add_storage, remove_storage | type (1), storage_id (8), write_version (16) |
 * | marker_begin, marker_durable, marker_invalidated_begin | type (1), epoch (8) |
 * | marker_end | type (1), epoch (8), CRC type (1) |
 *
 * - key_sid is the storage_id as 8 little-endian bytes, followed by the key.
 * - value_etc is the write_version (16) followed by the value. write_version is the epoch and the minor version,
 *   each as 8 little-endian bytes, in this order.
 * - limestone creates marker_end with no_crc as the CRC type, and the value is not checked when read.
 * - The entries in snapshot files and compacted WAL files are ordered by key_sid (compare_key_sid()).
 */

namespace limestone::internal::log_entry_format {

using limestone::api::epoch_id_type;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

/**
 * @brief The type of an entry. The value is the first byte of the entry.
 */
enum class entry_type : std::uint8_t {
    this_id_is_not_used = 0,

    // data management
    normal_entry = 1,
    normal_with_blob = 10,
    remove_entry = 5,

    // epoch management
    marker_begin = 2,
    marker_end = 3,
    marker_durable = 4,
    marker_invalidated_begin = 6,

    // storage management
    clear_storage = 7,
    add_storage = 8,
    remove_storage = 9,
};

/**
 * @brief Returns the string representation of an entry_type value.
 * @param type the entry type
 * @return the name of the entry type, or "unknown" for a value not defined in entry_type
 */
[[nodiscard]] std::string_view to_string_view(entry_type type) noexcept;

/**
 * @brief Writes the string representation of an entry_type value to a stream.
 * @param out the output stream
 * @param type the entry type
 * @return out
 */
std::ostream& operator<<(std::ostream& out, entry_type type);

/**
 * @brief Returns whether the type is a data entry type (normal_entry, normal_with_blob, remove_entry).
 * @param type the entry type
 * @return true if the type is a data entry type
 */
[[nodiscard]] bool is_data_entry(entry_type type) noexcept;

/**
 * @brief Returns whether the type is a storage entry type (clear_storage, add_storage, remove_storage).
 * @param type the entry type
 * @return true if the type is a storage entry type
 */
[[nodiscard]] bool is_storage_entry(entry_type type) noexcept;

/**
 * @brief Returns whether the type is a marker type (marker_begin, marker_end, marker_durable, marker_invalidated_begin).
 * @param type the entry type
 * @return true if the type is a marker type
 */
[[nodiscard]] bool is_marker(entry_type type) noexcept;

/**
 * @brief Returns whether the type byte is the value of a data entry type, a storage entry type or a marker type.
 *
 * Returns false for this_id_is_not_used (0) and for values not defined in entry_type.
 * @param value the value of the type byte
 * @return true if the value is a data entry type, a storage entry type or a marker type
 */
[[nodiscard]] bool is_valid_entry_type(std::uint8_t value) noexcept;

// sizes of the fields
inline constexpr std::size_t entry_type_size = 1;
inline constexpr std::size_t key_length_size = 4;
inline constexpr std::size_t value_length_size = 4;
inline constexpr std::size_t storage_id_size = 8;
inline constexpr std::size_t epoch_size = 8;
inline constexpr std::size_t write_version_epoch_size = 8;
inline constexpr std::size_t write_version_minor_size = 8;
inline constexpr std::size_t write_version_size = write_version_epoch_size + write_version_minor_size;
inline constexpr std::size_t blob_count_size = 4;
inline constexpr std::size_t blob_id_size = 8;
inline constexpr std::size_t crc_type_size = 1;

// sizes of each entry type, excluding the variable-length parts
inline constexpr std::size_t normal_entry_fixed_size =
    entry_type_size + key_length_size + value_length_size + storage_id_size + write_version_size;
inline constexpr std::size_t normal_with_blob_fixed_size = normal_entry_fixed_size + blob_count_size;
inline constexpr std::size_t remove_entry_fixed_size = entry_type_size + key_length_size + storage_id_size + write_version_size;
inline constexpr std::size_t storage_entry_size = entry_type_size + storage_id_size + write_version_size;
inline constexpr std::size_t marker_size = entry_type_size + epoch_size;
inline constexpr std::size_t marker_end_size = marker_size + crc_type_size;

static_assert(sizeof(storage_id_type) == storage_id_size);
static_assert(sizeof(epoch_id_type) == epoch_size);
static_assert(sizeof(limestone::api::blob_id_type) == blob_id_size);

/**
 * @brief Compares key_sids in the order of entries in snapshot files and compacted WAL files.
 *
 * Compares the serialized key_sids byte by byte from the first byte as unsigned values (0 to 255), and the first
 * differing byte decides the order. If one is a prefix of the other, the shorter one comes first.
 * Since the first 8 bytes of key_sid are the little-endian storage_id (see the format description), this order does
 * not match the numeric order of storage_ids.
 * @param a serialized key_sid
 * @param b serialized key_sid
 * @return negative if a comes before b, 0 if they are equal, positive if a comes after b
 */
[[nodiscard]] int compare_key_sid(std::string_view a, std::string_view b) noexcept;

/**
 * @brief Returns the oldest write_version (epoch 0, minor version 0).
 *
 * This is the write_version of the entries in compacted WAL files, and it compares as older than any other version.
 * @return the oldest write_version
 */
[[nodiscard]] write_version_type oldest_write_version();

/**
 * @brief Reads the first 4 bytes of src as a little-endian value.
 * @param src the source. It must be at least 4 bytes long
 * @return the value read
 */
[[nodiscard]] std::uint32_t load_uint32le(std::string_view src) noexcept;

/**
 * @brief Reads the first 8 bytes of src as a little-endian value.
 * @param src the source. It must be at least 8 bytes long
 * @return the value read
 */
[[nodiscard]] std::uint64_t load_uint64le(std::string_view src) noexcept;

/**
 * @brief Returns the position of key_sid in an entry (the offset from the beginning of the entry). Valid for data
 *        entries and storage entries.
 *
 * For storage entries, it is the position of storage_id.
 * @param type the entry type. Writes a FATAL log and aborts if it is neither a data entry nor a storage entry
 * @return the position of key_sid
 */
[[nodiscard]] std::size_t key_sid_offset(entry_type type);

/**
 * @brief Returns the serialized key_sid from the bytes of a complete data entry.
 *
 * Reads only the entry type and the key length. Does not parse the whole entry.
 * @param entry the bytes of a data entry. The first byte is the entry type, and the bytes must reach at least the
 *              end of the entry
 * @return the key_sid (a part of entry)
 */
[[nodiscard]] std::string_view data_entry_key_sid(std::string_view entry) noexcept;

/**
 * @brief Returns the write_version from the bytes of a complete data entry.
 *
 * Reads only the entry type and the key length. Does not parse the whole entry.
 * @param entry the bytes of a data entry. The first byte is the entry type, and the bytes must reach at least the
 *              end of the entry
 * @return the write_version
 */
[[nodiscard]] write_version_type data_entry_write_version(std::string_view entry) noexcept;

}  // namespace limestone::internal::log_entry_format
