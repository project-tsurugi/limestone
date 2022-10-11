/*
 * Copyright 2022-2022 tsurugi project.
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
#include <iostream>
#include <string>
#include <string_view>
#include <exception>

#include <boost/endian.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

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
    };
    
    log_entry() = default;

    static_assert(sizeof(epoch_id_type) == sizeof(uint64_t), "This code assumes that epoch_id_type is 64-bit width");
    static_assert(sizeof(storage_id_type) == sizeof(uint64_t), "This code assumes that storage_id_type is 64-bit width");

    static void begin_session(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_begin;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64(strm, static_cast<std::uint64_t>(epoch));
    }
    static void end_session(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_end;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64(strm, static_cast<std::uint64_t>(epoch));
    }
    static void durable_epoch(boost::filesystem::ofstream& strm, epoch_id_type epoch) {
        entry_type type = entry_type::marker_durable;
        write_uint8(strm, static_cast<std::uint8_t>(type));
        write_uint64(strm, static_cast<std::uint64_t>(epoch));
    }

// for writer (entry)
    void write(boost::filesystem::ofstream& strm) {
        switch(entry_type_) {
        case entry_type::normal_entry:
            write(strm, key_sid_, value_etc_);
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

    static void write(boost::filesystem::ofstream& strm, storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
        entry_type type = entry_type::normal_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::int32_t key_len = key.length();
        write_uint32(strm, static_cast<std::uint32_t>(key_len));

        std::int32_t value_len = value.length();
        write_uint32(strm, static_cast<std::uint32_t>(value_len));

        write_uint64(strm, static_cast<std::uint64_t>(storage_id));
        strm.write(key.data(), key_len);

        write_uint64(strm, static_cast<std::uint64_t>(write_version.epoch_number_));
        write_uint64(strm, static_cast<std::uint64_t>(write_version.minor_write_version_));
        strm.write(value.data(), value_len);
    }

    static void write(boost::filesystem::ofstream& strm, std::string_view key_sid, std::string_view value_etc) {
        entry_type type = entry_type::normal_entry;
        write_uint8(strm, static_cast<std::uint8_t>(type));

        std::int32_t key_len = key_sid.length() - sizeof(storage_id_type);
        write_uint32(strm, static_cast<std::uint32_t>(key_len));

        std::int32_t value_len = value_etc.length() - (sizeof(epoch_id_type) + sizeof(std::uint64_t));
        write_uint32(strm, static_cast<std::uint32_t>(value_len));

        strm.write(key_sid.data(), key_sid.length());
        strm.write(value_etc.data(), value_etc.length());
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
            std::int32_t key_len = read_uint32(strm);
            std::int32_t value_len = read_uint32(strm);

            key_sid_.resize(key_len + sizeof(storage_id_type));
            strm.read(key_sid_.data(), key_sid_.length());
            value_etc_.resize(value_len + sizeof(epoch_id_type) + sizeof(std::uint64_t));
            strm.read(value_etc_.data(), value_etc_.length());
            break;
        }
        case entry_type::marker_begin:
        case entry_type::marker_end:
        case entry_type::marker_durable:
            epoch_id_ = static_cast<epoch_id_type>(read_uint64(strm));
            break;

        case entry_type::this_id_is_not_used:
            return false;
        }

        return true;
    }

    void write_version(write_version_type& buf) {
        buf.epoch_number_ = boost::endian::load_little_u64(reinterpret_cast<unsigned char*>(value_etc_.data()));
        buf.minor_write_version_ = boost::endian::load_little_u64(reinterpret_cast<unsigned char*>(value_etc_.data()) + sizeof(epoch_id_type));
    }
    storage_id_type storage() {
        return boost::endian::load_little_u64(reinterpret_cast<const unsigned char*>(key_sid_.data()));
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
        return boost::endian::load_little_u64(reinterpret_cast<const unsigned char*>(value_etc.data()));
    }
    static std::uint64_t write_version_minor_write_version(std::string_view value_etc) {
        return boost::endian::load_little_u64(reinterpret_cast<const unsigned char*>(value_etc.data()) + sizeof(epoch_id_type));
    }

private:
    entry_type entry_type_{};
    epoch_id_type epoch_id_{};
    std::string key_sid_{};
    std::string value_etc_{};
    char one_char_{};

    static void write_uint8(std::ostream& out, const std::uint8_t value) {
        out.put(static_cast<char>(value));
    }
    static void write_uint32(std::ostream& out, const std::uint32_t value) {
        boost::endian::little_uint32_buf_t x{value};
        out.write(reinterpret_cast<const char*>(x.data()), 4);
    }
    static std::uint32_t read_uint32(std::istream& in) {
        boost::endian::little_uint32_buf_t x;
        in.read(reinterpret_cast<char*>(x.data()), 4);
        return x.value();
    }
    static void write_uint64(std::ostream& out, const std::uint64_t value) {
        boost::endian::little_uint64_buf_t x{value};
        out.write(reinterpret_cast<const char*>(x.data()), 8);
    }
    static std::uint64_t read_uint64(std::istream& in) {
        boost::endian::little_uint64_buf_t x;
        in.read(reinterpret_cast<char*>(x.data()), 8);
        return x.value();
    }
};

} // namespace limestone::api
