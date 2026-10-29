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
#include <vector>

#include <glog/logging.h>

#include <limestone/api/blob_id_type.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/logging.h>

#include "log_entry/log_entry_format.h"
#include "logging_helper.h"

namespace limestone::internal {

class log_entry_parse_result;

/**
 * @brief A view of one serialized entry.
 *
 * Does not own the region it refers to. It is valid only while that region is valid.
 * Except in the default-constructed state (type this_id_is_not_used), it refers only to a region that parse()
 * has parsed successfully.
 * An accessor writes a FATAL log and aborts if it is asked for a value that is not valid for the entry type.
 */
class log_entry_view {
public:
    using entry_type = log_entry_format::entry_type;
    using blob_id_type = limestone::api::blob_id_type;
    using epoch_id_type = limestone::api::epoch_id_type;
    using storage_id_type = limestone::api::storage_id_type;
    using write_version_type = limestone::api::write_version_type;

    /**
     * @brief Creates the default-constructed state (type this_id_is_not_used). In this state, all accessors are
     *        invalid.
     */
    log_entry_view() noexcept = default;

    /**
     * @brief Parses one entry from the beginning of a byte sequence.
     *
     * Does not allocate. The result is one of the following.
     * - ok: the entry is parsed to its end. Returns a view of the entry from the beginning of input, and the number
     *   of bytes consumed.
     * - short_entry: the bytes up to the end of the entry are not available. Returns the lower bound of the number
     *   of bytes needed to parse, counted from the beginning of input (including the bytes already in input).
     * - invalid_type: the first byte is not a valid entry type value (log_entry_format::is_valid_entry_type()).
     *   Returns that value.
     *
     * The lower bound for short_entry, counted from the beginning of input, is as follows.
     * - 1 if the type byte is not available.
     * - The size of the fixed part up to the type and the length fields, if the fixed part is not complete. For an
     *   entry type without length fields, the fixed part is the whole entry.
     * - For normal_entry and remove_entry, the size of the whole entry given by the length fields.
     * - For normal_with_blob, the size up to the end of value_etc plus the size of the number-of-BLOBs field until
     *   that field is read, and the size of the whole entry after it is read.
     * @param input the bytes to parse. The first byte is the entry type.
     * @return the result of parsing
     */
    [[nodiscard]] static log_entry_parse_result parse(std::string_view input) noexcept;

    /**
     * @brief Returns the view of the same bytes placed at another location. Does not parse again.
     * @param entry the same bytes as this entry (the destination of a copy or a move). Writes a FATAL log and aborts
     *              if its length differs. Does not check that the content is the same
     * @return the view that refers to entry, with the same type and the same lengths of the parts as this view. The
     *         default-constructed state for the default-constructed state
     */
    [[nodiscard]] log_entry_view relocated(std::string_view entry) const noexcept;

    /**
     * @brief Returns the entry type. Returns this_id_is_not_used in the default-constructed state.
     */
    [[nodiscard]] entry_type type() const noexcept;

    /**
     * @brief Returns the serialized value of the whole entry. Valid for all categories.
     */
    [[nodiscard]] std::string_view serialized_entry() const;

    /**
     * @brief Returns the serialized key_sid. Valid for data entries and storage entries.
     *
     * For storage entries, returns only the storage_id.
     */
    [[nodiscard]] std::string_view serialized_key_sid() const;

    /**
     * @brief Returns the serialized value_etc. Valid for data entries and storage entries.
     *
     * For remove_entry and storage entries, returns only the write_version.
     */
    [[nodiscard]] std::string_view serialized_value_etc() const;

    /**
     * @brief Returns the serialized blob_ids (without the number-of-BLOBs field). Valid for normal_with_blob.
     */
    [[nodiscard]] std::string_view serialized_blob_ids() const;

    /**
     * @brief Returns the storage_id. Valid for data entries and storage entries.
     */
    [[nodiscard]] storage_id_type storage() const;

    /**
     * @brief Returns the key. Valid for data entries.
     */
    [[nodiscard]] std::string_view key() const;

    /**
     * @brief Returns the value. Valid for normal_entry and normal_with_blob.
     */
    [[nodiscard]] std::string_view value() const;

    /**
     * @brief Returns the write_version. Valid for data entries and storage entries.
     */
    [[nodiscard]] write_version_type write_version() const;

    /**
     * @brief Returns the blob_ids. Valid for normal_with_blob.
     */
    [[nodiscard]] std::vector<blob_id_type> blob_ids() const;

    /**
     * @brief Returns the epoch of a marker. Valid for markers.
     */
    [[nodiscard]] epoch_id_type epoch_id() const;

private:
    /**
     * @brief Creates a view from the parsed region and the lengths of its parts. Called only by parse().
     */
    log_entry_view(std::string_view entry, entry_type type, std::uint32_t key_length, std::uint32_t value_length,
                   std::uint32_t blob_count) noexcept;

    /**
     * @brief Returns whether the entry is a data entry or a storage entry.
     */
    [[nodiscard]] bool is_data_or_storage() const noexcept;

    /**
     * @brief Returns the offset of key_sid in the entry. Called only for data entries and storage entries.
     */
    [[nodiscard]] std::size_t key_sid_offset() const noexcept;

    /**
     * @brief Returns the offset of value_etc in the entry. Called only for data entries and storage entries.
     */
    [[nodiscard]] std::size_t value_etc_offset() const noexcept;

    std::string_view entry_{};
    entry_type type_{entry_type::this_id_is_not_used};
    std::uint32_t key_length_{};
    std::uint32_t value_length_{};
    std::uint32_t blob_count_{};
};

/**
 * @brief The result of log_entry_view::parse().
 *
 * Created only by ok(), short_entry() and invalid_type(). Only the values valid for the result kind can be read; an
 * accessor writes a FATAL log and aborts if it is asked for a value that is not valid.
 */
class log_entry_parse_result {
public:
    /**
     * @brief The kind of the result.
     */
    enum class status : std::uint8_t {
        /// The entry is parsed to its end. view and consumed are valid.
        ok,
        /// The bytes up to the end of the entry are not available. required is valid.
        short_entry,
        /// The first byte is not a valid entry type value. type_value is valid.
        invalid_type,
    };

    /**
     * @brief Creates a result of successful parsing.
     * @param view the parsed entry. Writes a FATAL log and aborts if it is in the default-constructed state.
     * @return the result. The number of bytes consumed is the size of the region view refers to.
     */
    [[nodiscard]] static log_entry_parse_result ok(const log_entry_view& view);

    /**
     * @brief Creates a result in which the bytes up to the end of the entry are not available.
     * @param required the lower bound of the number of bytes needed to parse, counted from the beginning of input
     *                 and including the bytes already in input
     * @return the result
     */
    [[nodiscard]] static log_entry_parse_result short_entry(std::size_t required) noexcept;

    /**
     * @brief Creates a result in which the first byte is not a valid entry type value.
     * @param type_value the value of the first byte
     * @return the result
     */
    [[nodiscard]] static log_entry_parse_result invalid_type(std::uint8_t type_value) noexcept;

    /**
     * @brief Returns the kind of the result.
     */
    [[nodiscard]] status result() const noexcept;

    /**
     * @brief Returns whether parsing succeeded.
     *
     * Returns true if result is ok, and false if it is short_entry or invalid_type. This conversion does not
     * distinguish short_entry from invalid_type. A caller that needs to distinguish them refers to result().
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief Returns the parsed entry. Valid when the result kind is ok.
     */
    [[nodiscard]] log_entry_view view() const;

    /**
     * @brief Returns the number of bytes consumed (the size of the entry). Valid when the result kind is ok.
     */
    [[nodiscard]] std::size_t consumed() const;

    /**
     * @brief Returns the lower bound of the number of bytes needed to parse. Valid when the result kind is
     *        short_entry.
     *
     * The number is counted from the beginning of input and includes the bytes already in input.
     * The lower bound is greater than the size of input.
     */
    [[nodiscard]] std::size_t required() const;

    /**
     * @brief Returns the value of the first byte. Valid when the result kind is invalid_type.
     */
    [[nodiscard]] std::uint8_t type_value() const;

private:
    /**
     * @brief Creates a result from the values of its members. Called only by ok(), short_entry() and invalid_type().
     */
    log_entry_parse_result(status result, const log_entry_view& view, std::size_t required,
                           std::uint8_t type_value) noexcept;

    status result_{};
    log_entry_view view_{};
    std::size_t required_{};
    std::uint8_t type_value_{};
};

/**
 * @brief Returns the string representation of a log_entry_parse_result::status value.
 * @param result the result kind
 * @return the name of the result kind, or "unknown" for an undefined value
 */
[[nodiscard]] std::string_view to_string_view(log_entry_parse_result::status result) noexcept;

/**
 * @brief Writes the string representation of a log_entry_parse_result::status value to a stream.
 * @param out the output stream
 * @param result the result kind
 * @return out
 */
std::ostream& operator<<(std::ostream& out, log_entry_parse_result::status result);

// the accessors called for each entry in the sort and the merge. The cost of the calls takes a large part of the
// processing time, so they are defined here to allow inlining (moving them to the .cpp file makes the sort and the
// merge slower).

inline log_entry_view::entry_type log_entry_view::type() const noexcept {
    return type_;
}

inline std::string_view log_entry_view::serialized_entry() const {
    if (type_ == entry_type::this_id_is_not_used) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_;
}

inline std::string_view log_entry_view::serialized_key_sid() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    return entry_.substr(key_sid_offset(), log_entry_format::storage_id_size + key_length_);
}

inline log_entry_view::write_version_type log_entry_view::write_version() const {
    if (! is_data_or_storage()) {
        LOG_LP(FATAL) << "not valid for entry type " << type_;
    }
    const std::string_view version = entry_.substr(value_etc_offset());
    return {static_cast<epoch_id_type>(log_entry_format::load_uint64le(version)),
            log_entry_format::load_uint64le(version.substr(log_entry_format::write_version_epoch_size))};
}

inline bool log_entry_view::is_data_or_storage() const noexcept {
    return log_entry_format::is_data_entry(type_) || log_entry_format::is_storage_entry(type_);
}

inline std::size_t log_entry_view::key_sid_offset() const noexcept {
    return log_entry_format::key_sid_offset(type_);
}

inline std::size_t log_entry_view::value_etc_offset() const noexcept {
    return key_sid_offset() + log_entry_format::storage_id_size + key_length_;
}

}  // namespace limestone::internal
