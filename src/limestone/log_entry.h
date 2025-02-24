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

namespace limestone::api {

class datastore;

class log_entry {
public:
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

    static void begin_session(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_begin;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
    }
    static void end_session(FILE* strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_end;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64le(strm, static_cast<std::uint64_t>(epoch));
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
    void write(FILE* strm) {
        switch(entry_type_) {
        case entry_type::normal_entry:
            write(strm, key_sid_, value_etc_);
            break;
        case entry_type::normal_with_blob:
            write_with_blob(strm, key_sid_, value_etc_, blob_ids_);
            break;
        case entry_type::remove_entry:
            write_remove(strm, key_sid_, value_etc_);
            break;
        case entry_type::marker_begin:
            begin_session(strm, epoch_id_);
            break;
        case entry_type::marker_end:
            end_session(strm, epoch_id_);
            break;
        case entry_type::marker_durable:
            durable_epoch(strm, epoch_id_);
            break;
        case entry_type::marker_invalidated_begin:
            invalidated_begin(strm, epoch_id_);
            break;
        case entry_type::clear_storage:
            write_clear_storage(strm, key_sid_, value_etc_);
            break;
        case entry_type::add_storage:
            write_add_storage(strm, key_sid_, value_etc_);
            break;
        case entry_type::remove_storage:
            write_remove_storage(strm, key_sid_, value_etc_);
            break;
        case entry_type::this_id_is_not_used:
            break;
        }
    }

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
        write_uint64le(strm, static_cast<std::uint64_t>(write_version.minor_write_version_));
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
        write_uint64le(strm, static_cast<std::uint64_t>(write_version.minor_write_version_));
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
        write_uint64le(strm, static_cast<std::uint64_t>(write_version.minor_write_version_));
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
        write_uint64le(strm, static_cast<std::uint64_t>(write_version.minor_write_version_));
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

    bool read_entry_from(std::istream& strm, read_error& ec) { // NOLINT(readability-function-cognitive-complexity)
        ec.value(read_error::ok);
        ec.entry_type(entry_type::this_id_is_not_used);
        char one_char{};
        strm.read(&one_char, sizeof(char));
        entry_type_ = static_cast<entry_type>(one_char);
        if (strm.eof()) {
            return false;
        }

        switch(entry_type_) {
        case entry_type::normal_entry:
        {
            std::size_t key_len = read_uint32le(strm, ec);
            if (ec) return false;
            std::size_t value_len = read_uint32le(strm, ec);
            if (ec) return false;

            key_sid_.resize(key_len + sizeof(storage_id_type));
            read_bytes(strm, key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()), ec);
            if (ec) return false;
            value_etc_.resize(value_len + sizeof(epoch_id_type) + sizeof(std::uint64_t));
            read_bytes(strm, value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()), ec);
            if (ec) return false;
            break;
        }
        case entry_type::normal_with_blob:
        {
            std::size_t key_len = read_uint32le(strm, ec);
            if (ec) return false;
            std::size_t value_len = read_uint32le(strm, ec);
            if (ec) return false;

            key_sid_.resize(key_len + sizeof(storage_id_type));
            read_bytes(strm, key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()), ec);
            if (ec) return false;

            value_etc_.resize(value_len + sizeof(epoch_id_type) + sizeof(std::uint64_t));
            read_bytes(strm, value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()), ec);
            if (ec) return false;

            std::size_t blob_count = read_uint32le(strm, ec);
            if (ec) return false;

            blob_ids_.resize(blob_count * sizeof(blob_id_type));
            read_bytes(strm, blob_ids_.data(), static_cast<std::streamsize>(blob_ids_.size()), ec);
            if (ec) return false;
            break;
        }
        case entry_type::remove_entry:
        {
            std::size_t key_len = read_uint32le(strm, ec);
            if (ec) return false;

            key_sid_.resize(key_len + sizeof(storage_id_type));
            read_bytes(strm, key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()), ec);
            if (ec) return false;
            value_etc_.resize(sizeof(epoch_id_type) + sizeof(std::uint64_t));
            read_bytes(strm, value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()), ec);
            if (ec) return false;
            break;
        }
        case entry_type::clear_storage:
        case entry_type::add_storage:
        case entry_type::remove_storage:
        {
            key_sid_.resize(sizeof(storage_id_type));
            read_bytes(strm, key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()), ec);
            if (ec) return false;
            value_etc_.resize(sizeof(epoch_id_type) + sizeof(std::uint64_t));
            read_bytes(strm, value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()), ec);
            if (ec) return false;
            break;
        }
        case entry_type::marker_begin:
        case entry_type::marker_end:
        case entry_type::marker_durable:
        case entry_type::marker_invalidated_begin:
            epoch_id_ = static_cast<epoch_id_type>(read_uint64le(strm, ec));
            if (ec) return false;
            break;

        default:
            ec.value(read_error::unknown_type);
            ec.entry_type(entry_type_);
            return false;
        }

        return true;
    }

    void write_version(write_version_type& buf) const {
        buf.epoch_number_ = write_version_epoch_number(value_etc_);
        buf.minor_write_version_ = write_version_minor_write_version(value_etc_);
    }
    [[nodiscard]] storage_id_type storage() const {
        storage_id_type storage_id{};
        memcpy(static_cast<void*>(&storage_id), key_sid_.data(), sizeof(storage_id_type));
        return le64toh(storage_id);
    }
    void value(std::string& buf) const {
        buf = value_etc_.substr(sizeof(epoch_id_type) + sizeof(std::uint64_t));
    }
    void key(std::string& buf) const {
        buf = key_sid_.substr(sizeof(storage_id_type));
    }
    [[nodiscard]] entry_type type() const {
        return entry_type_;
    }
    [[nodiscard]] epoch_id_type epoch_id() const {
        return epoch_id_;
    }

    // for the purpose of storing key_sid and value_etc into LevelDB
    [[nodiscard]] const std::string& value_etc() const {
        return value_etc_;
    }
    [[nodiscard]] const std::string& key_sid() const {
        return key_sid_;
    }
    [[nodiscard]] const std::string& raw_blob_ids() const {
        return blob_ids_;
    }
    static epoch_id_type write_version_epoch_number(std::string_view value_etc) {
        epoch_id_type epoch_id{};
        memcpy(static_cast<void*>(&epoch_id), value_etc.data(), sizeof(epoch_id_type));
        return le64toh(epoch_id);
    }
    static std::uint64_t write_version_minor_write_version(std::string_view value_etc) {
        std::uint64_t minor_write_version{};
        memcpy(static_cast<void*>(&minor_write_version), value_etc.data() + sizeof(epoch_id_type), sizeof(std::uint64_t));
        return le64toh(minor_write_version);
    }
    static std::vector<blob_id_type> parse_blob_ids(std::string_view blob_ids_data) {
        std::vector<blob_id_type> blob_ids;
        const std::size_t blob_count = blob_ids_data.size() / sizeof(blob_id_type);
        blob_ids.reserve(blob_count);
        for (std::size_t i = 0; i < blob_count; ++i) {
            blob_id_type blob_id = 0;
            std::memcpy(&blob_id, blob_ids_data.data() + i * sizeof(blob_id_type), sizeof(blob_id_type));
            blob_ids.push_back(le64toh(blob_id));
        }
        return blob_ids;
    }    
    [[nodiscard]] std::vector<blob_id_type> get_blob_ids() const {
        return parse_blob_ids(blob_ids_);
    }

    /**
     * @brief Truncates the value portion of value_etc_, keeping only the write_version header,
     *        but only for entries of type normal_entry and normal_with_blob.
     *
     * For these entry types, value_etc_ contains the write_version header followed by the value.
     * This method removes any data beyond the header, leaving only the write_version information.
     * For other entry types, the method does nothing.
     */
    void truncate_value_from_normal_entry() {
        // Process only normal_entry and normal_with_blob entry types.
        if (entry_type_ != entry_type::normal_entry && entry_type_ != entry_type::normal_with_blob) {
            return;
        }

        constexpr std::size_t header_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
        if (value_etc_.size() > header_size) {
            value_etc_.resize(header_size);
        }
    }

    /**
     * @brief Creates a normal_with_blob type log_entry from raw data.
     *
     * This method constructs a log_entry instance specifically for entries of type normal_with_blob.
     * It initializes the key_sid, value_etc, and blob_ids fields from the provided parameters.
     * Note: The epoch_id field is not explicitly set by this method. If epoch_id is needed,
     * it should be extracted from the value_etc field (e.g. using write_version_epoch_number(value_etc)).
     *
     * @param key_sid A string view representing the key_sid field.
     * @param value_etc A string view representing the value_etc field.
     *                  This should contain the write_version header concatenated with the payload.
     * @param blob_ids A string view representing the blob_ids field (default is empty).
     * @return log_entry The constructed log_entry with type normal_with_blob.
     */
    static log_entry make_normal_with_blob_log_entry(
        std::string_view key_sid,
        std::string_view value_etc,
        std::string_view blob_ids = std::string_view()) {
        log_entry entry;
        entry.entry_type_ = entry_type::normal_with_blob;
        entry.key_sid_ = std::string(key_sid);
        entry.value_etc_ = std::string(value_etc);
        entry.blob_ids_ = std::string(blob_ids);
        return entry;
    }


private:
    entry_type entry_type_{};
    epoch_id_type epoch_id_{};
    std::string key_sid_{};
    std::string value_etc_{};
    std::string blob_ids_{};

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
    static std::uint32_t read_uint32le(std::istream& in, read_error& ec) {
        std::uint32_t buf{};
        read_bytes(in, &buf, sizeof(std::uint32_t), ec);
        return le32toh(buf);
    }
    static void write_uint64le(FILE* out, const std::uint64_t value) {
        std::uint64_t buf = htole64(value);
        write_bytes(out, &buf, sizeof(std::uint64_t));
    }
    static std::uint64_t read_uint64le(std::istream& in, read_error& ec) {
        std::uint64_t buf{};
        read_bytes(in, &buf, sizeof(std::uint64_t), ec);
        return le64toh(buf);
    }
    static void write_bytes(FILE* out, const void* buf, std::size_t len) {
        if (len == 0) return;  // nothing to write
        auto ret = fwrite(buf, len, 1, out);
        if (ret != 1) {
            LOG_AND_THROW_IO_EXCEPTION("fwrite failed", errno);
        }
    }
    static void read_bytes(std::istream& in, void* buf, std::streamsize len, read_error& ec) {
        in.read(reinterpret_cast<char*>(buf), len);  // NOLINT(*-reinterpret-cast)
        if (in.eof()) {
            ec.value(read_error::short_entry);
            return;
        }
    }
};

} // namespace limestone::api
