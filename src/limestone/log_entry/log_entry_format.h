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
#include <string_view>

#include <limestone/api/epoch_id_type.h>
#include <limestone/api/storage_id_type.h>

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
 *   The first 8 bytes of key_sid are the little-endian storage_id.
 * - value_etc is the write_version (16) followed by the value. write_version is the epoch and the minor version,
 *   each as 8 little-endian bytes, in this order.
 * - The CRC type of marker_end is written as no_crc, and its value is not checked when read.
 * - The entries in snapshot files and compacted WAL files are ordered by key_sid (compare_key_sid()).
 */

namespace limestone::internal::log_entry_format {

using limestone::api::epoch_id_type;
using limestone::api::storage_id_type;

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
[[nodiscard]] inline int compare_key_sid(std::string_view a, std::string_view b) noexcept {
    return a.compare(b);
}

}  // namespace limestone::internal::log_entry_format
