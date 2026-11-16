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
#include "locator/locator_writer.h"

#include <endian.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>

#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "locator/locator_format.h"
#include "log_entry/field_buffer.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_reader.h"
#include "logging_helper.h"

namespace limestone::internal {

namespace {

/**
 * @brief Makes the bytes of the header (the magic, the format version, and the reserved area filled with 0).
 * @return the bytes of the length of the header
 */
std::string header_bytes() {
    std::string header(locator_format::header_size, '\0');
    const std::uint64_t magic = htole64(locator_format::header_magic);
    const std::uint64_t version = htole64(locator_format::format_version);
    std::memcpy(header.data(), &magic, sizeof(magic));
    std::memcpy(&header[sizeof(magic)], &version, sizeof(version));
    return header;
}

}  // namespace

locator_writer::locator_writer(boost::filesystem::path file, std::uint64_t record_interval_bytes, bool sync_on_close)
    : file_(std::move(file), "locator file"), record_interval_bytes_(record_interval_bytes), sync_on_close_(sync_on_close) {
    if (record_interval_bytes_ == 0) {
        LOG_LP(FATAL) << "the record interval must not be 0: " << file_.file();
    }
    const std::string header = header_bytes();
    if (fwrite(header.data(), 1, header.size(), file_.stream()) != header.size()) {
        LOG_AND_THROW_IO_EXCEPTION("cannot write the header of locator file: " + file_.file().string(), errno);
    }
}

void locator_writer::note_entry(std::uint64_t offset) {
    if (! file_.is_open()) {
        LOG_LP(FATAL) << "already closed: " << file_.file();
    }
    if (offset < last_offset_) {
        LOG_LP(FATAL) << "the offset went backward from " << last_offset_ << " to " << offset << ": " << file_.file();
    }
    last_offset_ = offset;
    if (offset < next_threshold_) {
        return;
    }
    const std::uint64_t le = htole64(offset);
    if (fwrite(&le, 1, sizeof(le), file_.stream()) != sizeof(le)) {
        LOG_AND_THROW_IO_EXCEPTION("cannot write a record of locator file: " + file_.file().string(), errno);
    }
    ++record_count_;
    next_threshold_ = (offset / record_interval_bytes_ + 1) * record_interval_bytes_;
}

void locator_writer::close(std::uint64_t locatee_size) {
    if (locatee_size < last_offset_) {
        LOG_LP(FATAL) << "the locatee size " << locatee_size << " is smaller than the last offset " << last_offset_ << ": "
                      << file_.file();
    }
    field_buffer<locator_format::footer_size> footer;
    footer.append_uint64le(locatee_size);
    footer.append_uint64le(locator_format::footer_magic);
    if (fwrite(footer.data(), 1, footer.size(), file_.stream()) != footer.size()) {
        LOG_AND_THROW_IO_EXCEPTION("cannot write the footer of locator file: " + file_.file().string(), errno);
    }
    if (sync_on_close_) {
        file_.sync_and_close();
    } else {
        file_.close();
    }
}

std::size_t locator_writer::record_count() const noexcept {
    return record_count_;
}

std::size_t locator_writer::build_from_file(const boost::filesystem::path& locatee, const boost::filesystem::path& file,
                                            std::uint64_t record_interval_bytes) {
    using status = log_entry_read_result::status;
    log_entry_reader reader(locatee);
    locator_writer writer(file, record_interval_bytes, true);
    std::string previous_key_sid{};
    while (true) {
        const log_entry_read_result result = reader.next();
        if (result.result() == status::ok) {
            const log_entry_view view = result.view();
            if (log_entry_format::is_data_entry(view.type())) {
                // when the same key continues, the second and later entries are not recorded. The cursor takes the
                // first one, so a record pointing at a later one would lose the first one when the boundary of a
                // region is placed there
                const std::string_view key_sid = view.serialized_key_sid();
                if (key_sid != previous_key_sid) {
                    writer.note_entry(result.offset());
                    previous_key_sid.assign(key_sid);
                }
            }
            continue;
        }
        if (result.result() == status::end) {
            writer.close(result.offset());
            return writer.record_count();
        }
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO("the locatee file does not match the format at offset "
                                            + std::to_string(result.offset()) + ": " + locatee.string() + ": "
                                            + result.error().message());
    }
}

}  // namespace limestone::internal
