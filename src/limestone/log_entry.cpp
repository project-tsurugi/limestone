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

#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::api {

namespace format = limestone::internal::log_entry_format;
using limestone::internal::log_entry_view;

log_entry::log_entry(const log_entry_view& view) : entry_(view.serialized_entry()) {
    rebuild_view();
}

log_entry::log_entry(std::string entry) : entry_(std::move(entry)) {
    rebuild_view();
}

log_entry::log_entry(const log_entry& other) : entry_(other.entry_) {
    // a copied view_ would still refer to other.entry_, so rebuild it from this entry_
    rebuild_view();
}

log_entry& log_entry::operator=(const log_entry& other) {
    if (this != &other) {
        entry_ = other.entry_;
        rebuild_view();
    }
    return *this;
}

log_entry::log_entry(log_entry&& other) noexcept : entry_(std::exchange(other.entry_, {})) {
    // a short byte sequence is stored inside std::string (SSO) and moves to a new location, so rebuild view_
    rebuild_view();
    other.view_ = log_entry_view{};
}

log_entry& log_entry::operator=(log_entry&& other) noexcept {
    if (this != &other) {
        entry_ = std::exchange(other.entry_, {});
        rebuild_view();
        other.view_ = log_entry_view{};
    }
    return *this;
}

log_entry_view log_entry::view() const noexcept {
    return view_;
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

void log_entry::write(FILE* strm) const {
    const std::string_view entry = view_.serialized_entry();
    write_bytes(strm, entry.data(), entry.size());
}

bool log_entry::read_entry_from(std::istream& strm, read_error& ec) {
    ec.value(read_error::ok);
    ec.entry_type(entry_type::this_id_is_not_used);
    *this = log_entry{};

    // read from the type byte, and each time parsing returns short_entry, read up to the returned lower bound
    std::string buffer(format::entry_type_size, '\0');
    strm.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    // the end is detected by eof. If nothing is read without eof (such as a stream opened on a directory), the type
    // byte stays 0 and parsing gives unknown_type
    if (strm.eof()) {
        return false;
    }
    while (true) {
        const auto result = log_entry_view::parse(buffer);
        switch (result.result()) {
        case internal::log_entry_parse_result::status::ok:
            *this = log_entry{std::move(buffer)};
            return true;
        case internal::log_entry_parse_result::status::invalid_type:
            ec.value(read_error::unknown_type);
            ec.entry_type(static_cast<entry_type>(result.type_value()));
            return false;
        case internal::log_entry_parse_result::status::short_entry: {
            const std::size_t filled = buffer.size();
            buffer.resize(result.required());
            const auto requested = static_cast<std::streamsize>(buffer.size() - filled);
            strm.read(&buffer[filled], requested);
            if (strm.gcount() < requested) {
                ec.value(read_error::short_entry);
                ec.entry_type(static_cast<entry_type>(static_cast<std::uint8_t>(buffer[0])));
                return false;
            }
            break;
        }
        }
    }
}

void log_entry::write_version(write_version_type& buf) const {
    buf = view_.write_version();
}

storage_id_type log_entry::storage() const {
    return view_.storage();
}

void log_entry::value(std::string& buf) const {
    buf.assign(view_.value());
}

void log_entry::key(std::string& buf) const {
    buf.assign(view_.key());
}

log_entry::entry_type log_entry::type() const noexcept {
    return view_.type();
}

epoch_id_type log_entry::epoch_id() const {
    return view_.epoch_id();
}

std::string_view log_entry::value_etc() const {
    return view_.serialized_value_etc();
}

std::string_view log_entry::key_sid() const {
    return view_.serialized_key_sid();
}

std::string_view log_entry::raw_blob_ids() const {
    return view_.serialized_blob_ids();
}

std::vector<blob_id_type> log_entry::get_blob_ids() const {
    return view_.blob_ids();
}

void log_entry::truncate_value_from_normal_entry() {
    const entry_type type = view_.type();
    if (type != entry_type::normal_entry && type != entry_type::normal_with_blob) {
        return;
    }
    // copy the whole entry, remove the value, and set the value length to 0
    constexpr std::size_t value_length_offset = format::entry_type_size + format::key_length_size;
    const std::size_t value_offset =
        value_length_offset + format::value_length_size + view_.serialized_key_sid().size() + format::write_version_size;
    std::string buffer = entry_;
    buffer.erase(value_offset, view_.value().size());
    buffer.replace(value_length_offset, format::value_length_size, format::value_length_size, '\0');
    *this = log_entry{std::move(buffer)};
}

}  // namespace limestone::api
