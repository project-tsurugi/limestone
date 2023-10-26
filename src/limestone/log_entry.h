/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include <cstdio>
#include <endian.h>
#include <istream>
#include <string>
#include <string_view>
#include <exception>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>
#include <glog/logging.h>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/logging.h>
#include "logging_helper.h"

namespace limestone::api {

class datastore;

class log_entry {
public:
    enum class entry_type : std::uint8_t {
        this_id_is_not_used,
        normal_entry,
        marker_begin,
        marker_end,
        marker_durable,
        remove_entry,
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

// for writer (entry)
    void write(FILE* strm) {
        switch(entry_type_) {
        case entry_type::normal_entry:
            write(strm, key_sid_, value_etc_);
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

// for reader
    bool read(boost::filesystem::ifstream& strm) {
        strm.read(&one_char_, sizeof(char));
        entry_type_ = static_cast<entry_type>(one_char_);        
        if (strm.eof()) {
            return false;
        }

        switch(entry_type_) {
        case entry_type::normal_entry:
        {
            std::size_t key_len = read_uint32le(strm);
            std::size_t value_len = read_uint32le(strm);

            key_sid_.resize(key_len + sizeof(storage_id_type));
            strm.read(key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()));
            value_etc_.resize(value_len + sizeof(epoch_id_type) + sizeof(std::uint64_t));
            strm.read(value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()));
            break;
        }
        case entry_type::remove_entry:
        {
            std::size_t key_len = read_uint32le(strm);

            key_sid_.resize(key_len + sizeof(storage_id_type));
            strm.read(key_sid_.data(), static_cast<std::streamsize>(key_sid_.length()));
            value_etc_.resize(sizeof(epoch_id_type) + sizeof(std::uint64_t));
            strm.read(value_etc_.data(), static_cast<std::streamsize>(value_etc_.length()));
            break;
        }
        case entry_type::marker_begin:
        case entry_type::marker_end:
        case entry_type::marker_durable:
            epoch_id_ = static_cast<epoch_id_type>(read_uint64le(strm));
            break;

        case entry_type::this_id_is_not_used:
            return false;
        }

        return true;
    }

    void write_version(write_version_type& buf) {
        memcpy(static_cast<void*>(&buf), value_etc_.data(), sizeof(epoch_id_type) + sizeof(std::uint64_t));
    }
    storage_id_type storage() {
        storage_id_type storage_id{};
        memcpy(static_cast<void*>(&storage_id), key_sid_.data(), sizeof(storage_id_type));
        return storage_id;
    }
    void value(std::string& buf) {
        buf = value_etc_.substr(sizeof(epoch_id_type) + sizeof(std::uint64_t));
    }
    void key(std::string& buf) {
        buf = key_sid_.substr(sizeof(storage_id_type));
    }
    entry_type type() {
        return entry_type_;
    }
    [[nodiscard]] epoch_id_type epoch_id() const {
        return epoch_id_;
    }

    // for the purpose of storing key_sid and value_etc into LevelDB
    std::string& value_etc() {
        return value_etc_;
    }
    std::string& key_sid() {
        return key_sid_;
    }
    static epoch_id_type write_version_epoch_number(std::string_view value_etc) {
        epoch_id_type epoch_id{};
        memcpy(static_cast<void*>(&epoch_id), value_etc.data(), sizeof(epoch_id_type));
        return epoch_id;
    }
    static std::uint64_t write_version_minor_write_version(std::string_view value_etc) {
        std::uint64_t minor_write_version{};
        memcpy(static_cast<void*>(&minor_write_version), value_etc.data() + sizeof(epoch_id_type), sizeof(std::uint64_t));
        return minor_write_version;
    }

private:
    entry_type entry_type_{};
    epoch_id_type epoch_id_{};
    std::string key_sid_{};
    std::string value_etc_{};
    char one_char_{};

    static void write_uint8(FILE* out, const std::uint8_t value) {
        int ret = fputc(value, out);
        if (ret == EOF) {
            LOG_LP(ERROR) << "fputc failed, errno = " << errno;
            throw std::runtime_error("I/O error");
        }
    }
    static void write_uint32le(FILE* out, const std::uint32_t value) {
        std::uint32_t buf = htole32(value);
        write_bytes(out, &buf, sizeof(std::uint32_t));
    }
    static std::uint32_t read_uint32le(std::istream& in) {
        std::uint32_t buf{};
        in.read(reinterpret_cast<char*>(&buf), sizeof(std::uint32_t));  // NOLINT(*-reinterpret-cast)
        return le32toh(buf);
    }
    static void write_uint64le(FILE* out, const std::uint64_t value) {
        std::uint64_t buf = htole64(value);
        write_bytes(out, &buf, sizeof(std::uint64_t));
    }
    static std::uint64_t read_uint64le(std::istream& in) {
        std::uint64_t buf{};
        in.read(reinterpret_cast<char*>(&buf), sizeof(std::uint64_t));  // NOLINT(*-reinterpret-cast)
        return le64toh(buf);
    }
    static void write_bytes(FILE* out, const void* buf, std::size_t len) {
        if (len == 0) return;  // nothing to write
        auto ret = fwrite(buf, len, 1, out);
        if (ret != 1) {
            LOG_LP(ERROR) << "fwrite failed, errno = " << errno;
            throw std::runtime_error("I/O error");
        }
    }
};

} // namespace limestone::api
