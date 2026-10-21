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
#include <istream>
#include <string>
#include <string_view>
#include <exception>

#include <glog/logging.h>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/blob_id_type.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
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
     * @brief Writes the serialized value of the entry as is. Writes a FATAL log and aborts in the
     *        default-constructed state.
     * @param strm the output stream
     */
    void write(FILE* strm) const;

    static void write(FILE* strm, storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
        entry_type type = entry_type::normal_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::size_t key_len = key.length();
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        std::size_t value_len = value.length();
        assert(value_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(value_len));

        write_uint64le(strm, static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, key.data(), key_len);

        write_uint64le(strm, static_cast<std::uint64_t>(write_version.epoch_number_));
        write_uint64le(strm, write_version.minor_write_version_);
        write_bytes(strm, value.data(), value_len);
    }

    static void write(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        entry_type type = entry_type::normal_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::size_t key_len = key_sid.length() - sizeof(storage_id_type);
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        std::size_t value_len = value_etc.length() - (sizeof(epoch_id_type) + sizeof(std::uint64_t));
        assert(value_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(value_len));

        write_bytes(strm, key_sid.data(), key_sid.length());
        write_bytes(strm, value_etc.data(), value_etc.length());
    }

    static void write_with_blob(
        FILE* strm, 
        storage_id_type storage_id, 
        std::string_view key, 
        std::string_view value, 
        write_version_type write_version, 
        const std::vector<blob_id_type>& large_objects) 
    {
        entry_type type = entry_type::normal_with_blob;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::size_t key_len = key.length();
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        std::size_t value_len = value.length();
        assert(value_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(value_len));

        write_uint64le(strm, static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, key.data(), key_len);

        write_uint64le(strm, static_cast<std::uint64_t>(write_version.epoch_number_));
        write_uint64le(strm, write_version.minor_write_version_);
        write_bytes(strm, value.data(), value_len);

        // Write the number of BLOB references
        std::size_t blob_count = large_objects.size();
        assert(blob_count <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(blob_count));

        // Write each BLOB ID
        for (const auto& blob_id : large_objects) {
            write_uint64le(strm, static_cast<std::uint64_t>(blob_id));
        }
    }

    static void write_with_blob(FILE* strm, std::string_view key_sid, std::string_view value_etc, std::string_view blob_ids) {
        entry_type type = entry_type::normal_with_blob;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        // Calculate key_len
        std::size_t key_len = key_sid.length() - sizeof(storage_id_type);
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        // Calculate value_len
        std::size_t value_len = value_etc.length() - (sizeof(epoch_id_type) + sizeof(std::uint64_t));
        assert(value_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(value_len));

        // Write key_sid
        write_bytes(strm, key_sid.data(), key_sid.length());

        // Write value_etc
        write_bytes(strm, value_etc.data(), value_etc.length());

        // Calculate and write the number of BLOBs
        std::size_t blob_count = blob_ids.length() / sizeof(blob_id_type);
        assert(blob_count <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(blob_count));

        // Write the actual BLOB data
        write_bytes(strm, blob_ids.data(), blob_ids.length());
    }

    static void write_remove(FILE* strm, storage_id_type storage_id, std::string_view key, write_version_type write_version) {
        entry_type type = entry_type::remove_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::size_t key_len = key.length();
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        write_uint64le(strm, static_cast<std::uint64_t>(storage_id));
        write_bytes(strm, key.data(), key_len);

        write_uint64le(strm, static_cast<std::uint64_t>(write_version.epoch_number_));
        write_uint64le(strm, write_version.minor_write_version_);
    }

    static void write_remove(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        entry_type type = entry_type::remove_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::size_t key_len = key_sid.length() - sizeof(storage_id_type);
        assert(key_len <= UINT32_MAX);
        write_uint32le(strm, static_cast<std::uint32_t>(key_len));

        write_bytes(strm, key_sid.data(), key_sid.length());
        write_bytes(strm, value_etc.data(), value_etc.length());
    }

    static inline void write_ope_storage_common(FILE* strm, entry_type type, storage_id_type storage_id, write_version_type write_version) {
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(storage_id));
        write_uint64le(strm, static_cast<std::uint64_t>(write_version.epoch_number_));
        write_uint64le(strm, write_version.minor_write_version_);
    }

    static inline void write_ope_storage_common(FILE* strm, entry_type type, std::string_view key_sid, std::string_view value_etc) {
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_bytes(strm, key_sid.data(), key_sid.length());
        write_bytes(strm, value_etc.data(), value_etc.length());
    }

    static void write_clear_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::clear_storage, storage_id, write_version);
    }

    static void write_clear_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::clear_storage, key_sid, value_etc);
    }

    static void write_add_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::add_storage, storage_id, write_version);
    }

    static void write_add_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::add_storage, key_sid, value_etc);
    }

    static void write_remove_storage(FILE* strm, storage_id_type storage_id, write_version_type write_version) {
        write_ope_storage_common(strm, entry_type::remove_storage, storage_id, write_version);
    }

    static void write_remove_storage(FILE* strm, std::string_view key_sid, std::string_view value_etc) {
        write_ope_storage_common(strm, entry_type::remove_storage, key_sid, value_etc);
    }

// for reader
    bool read(std::istream& strm) {
        read_error ec{};
        bool rc = read_entry_from(strm, ec);
        if (ec) {
            LOG_AND_THROW_EXCEPTION("this log_entry is broken: " + ec.message());
        }
        return rc;
    }

    /**
     * @brief Reads one entry from a stream.
     *
     * On success, this object becomes the entry read. At the end of the stream or on failure, it is left in the
     * default-constructed state. The kind of failure and the entry type are set in ec.
     * @param strm the input stream
     * @param ec the result of reading
     * @return true if an entry is read, false at the end of the stream or on failure
     */
    bool read_entry_from(std::istream& strm, read_error& ec);

    /**
     * @brief Returns the write_version. Valid for data entries and storage entries.
     * @param buf the destination of the write_version
     */
    void write_version(write_version_type& buf) const;

    /**
     * @brief Returns the storage_id. Valid for data entries and storage entries.
     * @return the storage_id
     */
    [[nodiscard]] storage_id_type storage() const;

    /**
     * @brief Returns the value. Valid for normal_entry and normal_with_blob.
     * @param buf the destination of the value
     */
    void value(std::string& buf) const;

    /**
     * @brief Returns the key. Valid for data entries.
     * @param buf the destination of the key
     */
    void key(std::string& buf) const;

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

    /**
     * @brief Returns the serialized value_etc. Valid for data entries and storage entries.
     *
     * For remove_entry and storage entries, returns only the write_version. The return value is valid as long as a
     * view returned by view().
     * @return the serialized value_etc
     */
    [[nodiscard]] std::string_view value_etc() const;

    /**
     * @brief Returns the serialized key_sid. Valid for data entries and storage entries.
     *
     * For storage entries, returns only the storage_id. The return value is valid as long as a view returned by
     * view().
     * @return the serialized key_sid
     */
    [[nodiscard]] std::string_view key_sid() const;

    /**
     * @brief Returns the serialized blob_ids (without the number-of-BLOBs field). Valid for normal_with_blob.
     *
     * The return value is valid as long as a view returned by view().
     * @return the serialized blob_ids
     */
    [[nodiscard]] std::string_view raw_blob_ids() const;

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
    static std::vector<blob_id_type> parse_blob_ids(std::string_view blob_ids_data) {
        std::vector<blob_id_type> blob_ids;
        const std::size_t blob_count = blob_ids_data.size() / sizeof(blob_id_type);
        blob_ids.reserve(blob_count);
        for (std::size_t i = 0; i < blob_count; ++i) {
            blob_id_type blob_id = 0;
            std::memcpy(&blob_id, blob_ids_data.data() + i * sizeof(blob_id_type), sizeof(blob_id_type));  // NOLINT(*-pointer-arithmetic)
            blob_ids.push_back(le64toh(blob_id));
        }
        return blob_ids;
    }    
    /**
     * @brief Returns the blob_ids. Valid for normal_with_blob.
     * @return the blob_ids
     */
    [[nodiscard]] std::vector<blob_id_type> get_blob_ids() const;

    /**
     * @brief Empties the value of normal_entry and normal_with_blob.
     *
     * value_etc becomes only the write_version, and the entry type, key_sid and blob_ids do not change. Does
     * nothing for other entry types.
     */
    void truncate_value_from_normal_entry();

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
    // the view of entry_; rebuilt by rebuild_view() whenever the content or the location of entry_ changes
    limestone::internal::log_entry_view view_{};

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
