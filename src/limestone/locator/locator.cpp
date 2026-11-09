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
#include "locator/locator.h"

#include <cerrno>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <sys/stat.h>

#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "locator/locator_format.h"
#include "log_entry/log_entry_format.h"
#include "logging_helper.h"

namespace limestone::internal {

locator::locator(boost::filesystem::path file, std::uint64_t locatee_size) : file_(std::move(file)) {
    real_file_operations file_ops{};
    read(file_ops, locatee_size);
}

locator::locator(boost::filesystem::path file, std::uint64_t locatee_size, file_operations& file_ops)
    : file_(std::move(file)) {
    read(file_ops, locatee_size);
}

void locator::read(file_operations& file_ops, std::uint64_t locatee_size) {
    using locator_format::footer_magic;
    using locator_format::footer_size;
    using locator_format::format_version;
    using locator_format::header_magic;
    using locator_format::header_size;
    using locator_format::record_size;
    using log_entry_format::load_uint64le;

    const auto closer = [&file_ops](FILE* s) { (void) file_ops.fclose(s); };
    const std::unique_ptr<FILE, decltype(closer)> stream(file_ops.fopen(file_.c_str(), "rb"), closer);
    if (! stream) {
        LOG_AND_THROW_IO_EXCEPTION("cannot open locator file: " + file_.string(), errno);
    }

    // ヘッダだけ先に読んで検査する。無関係なファイルを丸ごと読まないため
    std::string header(header_size, '\0');
    if (file_ops.fread(header.data(), 1, header_size, stream.get()) != header_size) {
        const int err = errno;
        if (file_ops.ferror(stream.get()) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("cannot read locator file: " + file_.string(), err);
        }
        LOG_AND_THROW_EXCEPTION("invalid locator file (shorter than the header): " + file_.string());
    }
    if (load_uint64le(std::string_view(header).substr(0, 8)) != header_magic) {
        LOG_AND_THROW_EXCEPTION("invalid locator file (the magic of the header does not match): " + file_.string());
    }
    const std::uint64_t version = load_uint64le(std::string_view(header).substr(8, 8));
    if (version != format_version) {
        LOG_AND_THROW_EXCEPTION("invalid locator file (the format version " + std::to_string(version) + " is not "
                                + std::to_string(format_version) + "): " + file_.string());
    }

    // 大きさを検査してから、残り (レコードの列と終端) を読む
    struct stat st {};
    if (file_ops.fstat(file_ops.fileno(stream.get()), &st) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("cannot stat locator file: " + file_.string(), errno);
    }
    const auto file_size = static_cast<std::uint64_t>(st.st_size);
    if (file_size < header_size + footer_size || (file_size - header_size - footer_size) % record_size != 0) {
        LOG_AND_THROW_EXCEPTION("invalid locator file (the size " + std::to_string(file_size)
                                + " is not header + records + footer): " + file_.string());
    }
    std::string rest(file_size - header_size, '\0');
    if (file_ops.fread(rest.data(), 1, rest.size(), stream.get()) != rest.size()) {
        const int err = errno;
        if (file_ops.ferror(stream.get()) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("cannot read locator file: " + file_.string(), err);
        }
        LOG_AND_THROW_EXCEPTION("invalid locator file (shorter than its size " + std::to_string(file_size) + "): "
                                + file_.string());
    }
    const std::string_view bytes(rest);
    const std::string_view footer = bytes.substr(bytes.size() - footer_size);
    if (load_uint64le(footer.substr(8, 8)) != footer_magic) {
        LOG_AND_THROW_EXCEPTION("invalid locator file (the magic of the footer does not match): " + file_.string());
    }
    locatee_size_ = load_uint64le(footer.substr(0, 8));
    if (locatee_size_ != locatee_size) {
        LOG_AND_THROW_EXCEPTION("invalid locator file (the locatee size " + std::to_string(locatee_size_)
                                + " in the footer does not match the actual size " + std::to_string(locatee_size)
                                + "): " + file_.string());
    }

    const std::string_view records = bytes.substr(0, bytes.size() - footer_size);
    positions_.reserve(records.size() / record_size);
    for (std::size_t pos = 0; pos < records.size(); pos += record_size) {
        positions_.push_back(load_uint64le(records.substr(pos, record_size)));
    }
}

std::size_t locator::record_count() const noexcept {
    return positions_.size();
}

std::uint64_t locator::position(std::size_t k) const {
    if (k >= positions_.size()) {
        LOG_LP(FATAL) << "the record number " << k << " is out of range (" << positions_.size() << " records): "
                      << file_;
    }
    return positions_[k];
}

const std::vector<std::uint64_t>& locator::positions() const noexcept {
    return positions_;
}

std::uint64_t locator::locatee_size() const noexcept {
    return locatee_size_;
}

const boost::filesystem::path& locator::file() const noexcept {
    return file_;
}

}  // namespace limestone::internal
