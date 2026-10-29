/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <cassert>
#include <cstdio>
#include <endian.h>
#include <string>
#include <string_view>
#include <vector>
#include <exception>

#include <glog/logging.h>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/blob_id_type.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "log_entry/field_buffer.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_view.h"

namespace limestone::api {

class datastore;

class log_entry {
public:
    using entry_type = limestone::internal::log_entry_format::entry_type;
    enum class crc_type : std::uint8_t {
        // use non-zero values to avoid misinterpretation of zero-filled junk
        no_crc = 1,
        // future values should be added here when implementation is ready
    };

    /**
     * @brief How write_serialized() writes the write_version.
     */
    enum class write_version_handling : std::uint8_t {
        /// Writes the write_version as is.
        keep,
        /// Writes the oldest write_version (log_entry_format::oldest_write_version()) instead.
        oldest,
    };

    class read_error {
    public:
        enum code {
            ok = 0,
            // warning
            nondurable_snippet = 0x01,
            // error
            short_entry = 0x81,
            // unknown type; eg. type 0
            unknown_type = 0x82,
            // unexpected type; eg. add_entry at the head of pwal file or in epoch file
            unexpected_type = 0x83,
        };

        read_error() noexcept : value_(ok) {}
        explicit read_error(code value) noexcept : value_(value) {}
        read_error(code value, log_entry::entry_type entry_type) noexcept : value_(value), entry_type_(entry_type) {}

        void value(code value) noexcept { value_ = value; }
        [[nodiscard]] code value() const noexcept { return value_; }
        void entry_type(log_entry::entry_type entry_type) noexcept { entry_type_ = entry_type; }
        [[nodiscard]] log_entry::entry_type entry_type() const noexcept { return entry_type_; }

        explicit operator bool() const noexcept { return value_ != 0; }

        [[nodiscard]] std::string message() const {
            switch (value_) {
            case ok: return "no error";
            case nondurable_snippet: return "found nondurable epoch snippet";
            case short_entry: return "unexpected EOF";
            case unknown_type: return "unknown log_entry type " + std::to_string(static_cast<int>(entry_type_));
            case unexpected_type: return "unexpected log_entry type " + std::to_string(static_cast<int>(entry_type_));
            }
            return "unknown error code " + std::to_string(value_);
        }
    private:
        code value_;
        log_entry::entry_type entry_type_{0};
    };

    /**
     * @brief Creates an entry in the default-constructed state (type this_id_is_not_used, with all values invalid).
     */
    log_entry() = default;

    /**
     * @brief Creates an entry by copying the bytes of the entry the view refers to.
     * @param view the source entry. Writes a FATAL log and aborts if it is in the default-constructed state
     */
    explicit log_entry(const limestone::internal::log_entry_view& view);

    /**
     * @brief Creates a copy.
     * @param other the source
     */
    log_entry(const log_entry& other);

    /**
     * @brief Assigns a copy.
     * @param other the source
     * @return *this
     */
    log_entry& operator=(const log_entry& other);

    /**
     * @brief Creates an entry by moving. The source is left in the default-constructed state (type
     *        this_id_is_not_used, with all values invalid).
     * @param other the source
     */
    log_entry(log_entry&& other) noexcept;

    /**
     * @brief Assigns by moving. The source is left in the default-constructed state (type this_id_is_not_used,
     *        with all values invalid).
     * @param other the source
     * @return *this
     */
    log_entry& operator=(log_entry&& other) noexcept;

    /**
     * @brief Destroys the entry.
     */
    ~log_entry() = default;

    /**
     * @brief Returns a view of this entry.
     *
     * The returned view is valid until this object is destroyed, assigned or modified. In the default-constructed
     * state, returns a view in the default-constructed state.
     * @return a view of this entry
     */
    [[nodiscard]] limestone::internal::log_entry_view view() const noexcept;

    /**
     * @brief Creates a normal_entry.
     * @param storage_id storage_id
     * @param key the key. Writes a FATAL log and aborts if its length does not fit in 32 bits
     * @param value the value. Writes a FATAL log and aborts if its length does not fit in 32 bits
     * @param write_version write_version
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_normal_entry(storage_id_type storage_id, std::string_view key,
                                                     std::string_view value, write_version_type write_version);

    /**
     * @brief Creates a normal_with_blob.
     * @param storage_id storage_id
     * @param key the key. Writes a FATAL log and aborts if its length does not fit in 32 bits
     * @param value the value. Writes a FATAL log and aborts if its length does not fit in 32 bits
     * @param write_version write_version
     * @param blob_ids the blob_ids. Writes a FATAL log and aborts if their number does not fit in 32 bits
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_normal_with_blob(storage_id_type storage_id, std::string_view key,
                                                         std::string_view value, write_version_type write_version,
                                                         const std::vector<blob_id_type>& blob_ids);

    /**
     * @brief Creates a remove_entry.
     * @param storage_id storage_id
     * @param key the key. Writes a FATAL log and aborts if its length does not fit in 32 bits
     * @param write_version write_version
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_remove_entry(storage_id_type storage_id, std::string_view key,
                                                     write_version_type write_version);

    /**
     * @brief Creates a clear_storage.
     * @param storage_id storage_id
     * @param write_version write_version
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_clear_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief Creates an add_storage.
     * @param storage_id storage_id
     * @param write_version write_version
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_add_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief Creates a remove_storage.
     * @param storage_id storage_id
     * @param write_version write_version
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_remove_storage(storage_id_type storage_id, write_version_type write_version);

    /**
     * @brief Creates a marker_begin.
     * @param epoch epoch
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_marker_begin(epoch_id_type epoch);

    /**
     * @brief Creates a marker_end. The CRC type is no_crc.
     * @param epoch epoch
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_marker_end(epoch_id_type epoch);

    /**
     * @brief Creates a marker_durable.
     * @param epoch epoch
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_marker_durable(epoch_id_type epoch);

    /**
     * @brief Creates a marker_invalidated_begin.
     * @param epoch epoch
     * @return the created entry
     */
    [[nodiscard]] static log_entry make_marker_invalidated_begin(epoch_id_type epoch);

    static void begin_session(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_begin;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
    }
    static void end_session(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_end;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
        write_uint8(strm, static_cast<std::uint8_t>(crc_type::no_crc)); 
    }
    static void durable_epoch(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_durable;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
    }
    static void invalidated_begin(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_invalidated_begin;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
    }

// for writer (entry)
    /**
     * @brief Writes the serialized value of the entry the view refers to.
     * @param strm the output stream
     * @param entry the entry to write. Writes a FATAL log and aborts if it is in the default-constructed state
     * @param handling how to write the write_version. oldest is valid for data entries and storage entries, and
     *                 writes a FATAL log and aborts for markers
     * @throws limestone_io_exception if writing fails
     */
    static void write_serialized(FILE* strm, const limestone::internal::log_entry_view& entry,
                                 write_version_handling handling = write_version_handling::keep);

    static void write(FILE* strm, storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
        std::size_t key_len = key.length();
        std::size_t value_len = value.length();
        // all the lengths are checked before writing the first byte; the exception for a violation is caught by the
        // public API of log_channel (HANDLE_EXCEPTION_AND_ABORT), which aborts
        if (key_len > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("key length " + std::to_string(key_len) + " does not fit in 32 bits");
        }
        if (value_len > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("value length " + std::to_string(value_len) + " does not fit in 32 bits");
        }

        // the fixed-length fields that come one after another are written at once (to reduce the calls of fwrite)
        limestone::internal::field_buffer<normal_entry_head_size> head{};
        head.append_uint8(static_cast<std::uint8_t>(entry_type::normal_entry));
        head.append_uint32le(static_cast<std::uint32_t>(key_len));
        head.append_uint32le(static_cast<std::uint32_t>(value_len));
        head.append_uint64le(static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, head.data(), head.size());
        write_bytes(strm, key.data(), key_len);
        write_write_version(strm, write_version);
        write_bytes(strm, value.data(), value_len);
    }

    static void write_with_blob(
        FILE* strm, 
        storage_id_type storage_id, 
        std::string_view key, 
        std::string_view value, 
        write_version_type write_version, 
        const std::vector<blob_id_type>& large_objects) 
    {
        std::size_t key_len = key.length();
        std::size_t value_len = value.length();
        std::size_t blob_count = large_objects.size();
        // all the lengths are checked before writing the first byte; the exception for a violation is caught by the
        // public API of log_channel (HANDLE_EXCEPTION_AND_ABORT), which aborts
        if (key_len > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("key length " + std::to_string(key_len) + " does not fit in 32 bits");
        }
        if (value_len > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("value length " + std::to_string(value_len) + " does not fit in 32 bits");
        }
        if (blob_count > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("blob count " + std::to_string(blob_count) + " does not fit in 32 bits");
        }

        limestone::internal::field_buffer<normal_entry_head_size> head{};
        head.append_uint8(static_cast<std::uint8_t>(entry_type::normal_with_blob));
        head.append_uint32le(static_cast<std::uint32_t>(key_len));
        head.append_uint32le(static_cast<std::uint32_t>(value_len));
        head.append_uint64le(static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, head.data(), head.size());
        write_bytes(strm, key.data(), key_len);
        write_write_version(strm, write_version);
        write_bytes(strm, value.data(), value_len);

        // Write the number of BLOB references
        write_uint32le(strm, static_cast<std::uint32_t>(blob_count));

        // Write each BLOB ID
        for (const auto& blob_id : large_objects) {
            write_uint64le(strm, static_cast<std::uint64_t>(blob_id));
        }
    }

    static void write_remove(FILE* strm, storage_id_type storage_id, std::string_view key, write_version_type write_version) {
        std::size_t key_len = key.length();
        // the length is checked before writing the first byte; the exception for a violation is caught by the public
        // API of log_channel (HANDLE_EXCEPTION_AND_ABORT), which aborts
        if (key_len > UINT32_MAX) {
            LOG_AND_THROW_EXCEPTION("key length " + std::to_string(key_len) + " does not fit in 32 bits");
        }

        limestone::internal::field_buffer<remove_entry_head_size> head{};
        head.append_uint8(static_cast<std::uint8_t>(entry_type::remove_entry));
        head.append_uint32le(static_cast<std::uint32_t>(key_len));
        head.append_uint64le(static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, head.data(), head.size());
        write_bytes(strm, key.data(), key_len);
        write_write_version(strm, write_version);
    }

    static inline void write_ope_storage_common(FILE* strm, entry_type type, storage_id_type storage_id, write_version_type write_version) {
        // a storage entry is entirely fixed-length, so it is written at once
        limestone::internal::field_buffer<limestone::internal::log_entry_format::storage_entry_size> entry{};
        entry.append_uint8(static_cast<std::uint8_t>(type));
        entry.append_uint64le(static_cast<std::uint64_t>(storage_id));
        entry.append_uint64le(static_cast<std::uint64_t>(write_version.epoch_number_));
        entry.append_uint64le(write_version.minor_write_version_);
        write_bytes(strm, entry.data(), entry.size());
    }

    static void write_clear_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::clear_storage, storage_id, write_version);
    }

    static void write_add_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::add_storage, storage_id, write_version);
    }

    static void write_remove_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::remove_storage, storage_id, write_version);
    }

// for reader
    /**
     * @brief Returns the storage_id. Valid for data entries and storage entries.
     * @return the storage_id
     */
    [[nodiscard]] storage_id_type storage() const;

    /**
     * @brief Returns the entry type. Returns this_id_is_not_used in the default-constructed state.
     * @return the entry type
     */
    [[nodiscard]] entry_type type() const noexcept;

    /**
     * @brief Returns the epoch of a marker. Valid for markers.
     * @return the epoch
     */
    [[nodiscard]] epoch_id_type epoch_id() const;

    static epoch_id_type write_version_epoch_number(std::string_view value_etc) {
        epoch_id_type epoch_id{};
        memcpy(static_cast<void*>(&epoch_id), value_etc.data(), sizeof(epoch_id_type));
        return le64toh(epoch_id);
    }
    static std::uint64_t write_version_minor_write_version(std::string_view value_etc) {
        std::uint64_t minor_write_version{};
        memcpy(static_cast<void*>(&minor_write_version), value_etc.data() + sizeof(epoch_id_type), sizeof(std::uint64_t));  // NOLINT(*-pointer-arithmetic)
        return le64toh(minor_write_version);
    }

    /**
     * @brief Returns the serialized value of the whole entry. Valid except in the default-constructed state.
     *
     * The return value is valid as long as a view returned by view() is valid.
     * @return the serialized value of the whole entry (from the type byte to the end)
     */
    [[nodiscard]] std::string_view serialized_entry() const;

    /**
     * @brief Returns the serialized key_sid. Valid for data entries and storage entries.
     *
     * For storage entries, returns only the storage_id. The return value is valid as long as a view returned by
     * view() is valid.
     * @return the serialized key_sid
     */
    [[nodiscard]] std::string_view serialized_key_sid() const;

    /**
     * @brief Returns the serialized value_etc. Valid for data entries and storage entries.
     *
     * For remove_entry and storage entries, returns only the write_version. The return value is valid as long as a
     * view returned by view() is valid.
     * @return the serialized value_etc
     */
    [[nodiscard]] std::string_view serialized_value_etc() const;

    /**
     * @brief Returns the serialized blob_ids (without the number-of-BLOBs field). Valid for normal_with_blob.
     *
     * The return value is valid as long as a view returned by view() is valid.
     * @return the serialized blob_ids
     */
    [[nodiscard]] std::string_view serialized_blob_ids() const;

    /**
     * @brief Returns the key. Valid for data entries.
     *
     * The return value is valid as long as a view returned by view() is valid.
     * @return the key
     */
    [[nodiscard]] std::string_view key() const;

    /**
     * @brief Returns the value. Valid for normal_entry and normal_with_blob.
     *
     * The return value is valid as long as a view returned by view() is valid.
     * @return the value
     */
    [[nodiscard]] std::string_view value() const;

    /**
     * @brief Returns the write_version. Valid for data entries and storage entries.
     * @return the write_version
     */
    [[nodiscard]] write_version_type write_version() const;

    /**
     * @brief Returns the blob_ids. Valid for normal_with_blob.
     * @return the blob_ids
     */
    [[nodiscard]] std::vector<blob_id_type> blob_ids() const;

private:
    /**
     * @brief Creates an entry from the serialized value of one entry.
     * @param entry the bytes of the entry. If empty, creates the default-constructed state. Writes a FATAL log and
     *              aborts if it does not follow the format
     */
    explicit log_entry(std::string entry);

    /**
     * @brief Parses entry_ and rebuilds view_. If entry_ is empty, sets view_ to the default-constructed state.
     */
    void rebuild_view();

    // the serialized value of one entry (from the type byte to the end); empty in the default-constructed state
    std::string entry_{};
    // the view of entry_; rebuilt by rebuild_view() when the content changes, and relocated when only the location
    // changes (copy and move)
    limestone::internal::log_entry_view view_{};

    // the sizes of the leading fixed-length parts written at once by the typed writers (normal_entry and
    // normal_with_blob, and remove_entry)
    static constexpr std::size_t normal_entry_head_size =
            limestone::internal::log_entry_format::entry_type_size
            + limestone::internal::log_entry_format::key_length_size
            + limestone::internal::log_entry_format::value_length_size
            + limestone::internal::log_entry_format::storage_id_size;
    static constexpr std::size_t remove_entry_head_size =
            limestone::internal::log_entry_format::entry_type_size
            + limestone::internal::log_entry_format::key_length_size
            + limestone::internal::log_entry_format::storage_id_size;

    /**
     * @brief Writes a write_version (the epoch and the minor version) at once.
     * @param out the output stream
     * @param write_version the write_version to write
     * @throws limestone_io_exception if writing fails
     */
    static void write_write_version(FILE* out, const write_version_type& write_version) {
        limestone::internal::field_buffer<limestone::internal::log_entry_format::write_version_size> buf{};
        buf.append_uint64le(static_cast<std::uint64_t>(write_version.epoch_number_));
        buf.append_uint64le(write_version.minor_write_version_);
        write_bytes(out, buf.data(), buf.size());
    }

    static void write_uint8(FILE* out, const std::uint8_t value) {
        int ret = fputc(value, out);
        if (ret == EOF) {
            LOG_AND_THROW_IO_EXCEPTION("fputc failed", errno);
        }
    }
    static void write_uint32le(FILE* out, const std::uint32_t value) {
        std::uint32_t buf = htole32(value);
        write_bytes(out, &buf, sizeof(std::uint32_t));
    }
    static void write_uint64le(FILE* out, const std::uint64_t value) {
        std::uint64_t buf = htole64(value);
        write_bytes(out, &buf, sizeof(std::uint64_t));
    }
    static void write_bytes(FILE* out, const void* buf, std::size_t len) {
        if (len == 0) return;  // nothing to write
        auto ret = fwrite(buf, len, 1, out);
        if (ret != 1) {
            LOG_AND_THROW_IO_EXCEPTION("fwrite failed", errno);
        }
    }
};

} // namespace limestone::api
