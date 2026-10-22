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

#include "log_entry/log_entry_reader.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

log_entry_read_result::log_entry_read_result(status result, const log_entry_view& view, std::uint64_t offset,
                                             const log_entry::read_error& error) noexcept
    : result_(result), view_(view), offset_(offset), error_(error) {}

log_entry_read_result log_entry_read_result::ok(const log_entry_view& view, std::uint64_t offset) {
    if (view.type() == log_entry_view::entry_type::this_id_is_not_used) {
        LOG_LP(FATAL) << "default-constructed view";
    }
    return {status::ok, view, offset, log_entry::read_error{}};
}

log_entry_read_result log_entry_read_result::end(std::uint64_t offset) noexcept {
    return {status::end, log_entry_view{}, offset, log_entry::read_error{}};
}

log_entry_read_result log_entry_read_result::error(const log_entry::read_error& error, std::uint64_t offset) noexcept {
    return {status::error, log_entry_view{}, offset, error};
}

log_entry_read_result::status log_entry_read_result::result() const noexcept {
    return result_;
}

log_entry_view log_entry_read_result::view() const {
    if (result_ != status::ok) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return view_;
}

std::uint64_t log_entry_read_result::offset() const noexcept {
    return offset_;
}

log_entry::read_error log_entry_read_result::error() const {
    if (result_ != status::error) {
        LOG_LP(FATAL) << "not valid for result " << result_;
    }
    return error_;
}

std::string_view to_string_view(log_entry_read_result::status result) noexcept {
    switch (result) {
        case log_entry_read_result::status::ok: return "ok";
        case log_entry_read_result::status::end: return "end";
        case log_entry_read_result::status::error: return "error";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& out, log_entry_read_result::status result) {
    return out << to_string_view(result);
}

log_entry_reader::log_entry_reader(const boost::filesystem::path& file, std::uint64_t start_offset)
    : file_ops_(&real_file_ops_), file_(file) {
    open_file(start_offset);
}

log_entry_reader::log_entry_reader(const boost::filesystem::path& file, std::uint64_t start_offset,
                                   file_write_state_function state)
    : file_ops_(&real_file_ops_), file_(file), state_(std::move(state)), writing_(true) {
    if (! state_) {
        LOG_LP(FATAL) << "empty write state function";
    }
    buffer_offset_ = start_offset;
    open_if_exists();
}

log_entry_reader::log_entry_reader(const boost::filesystem::path& file, std::uint64_t start_offset,
                                   file_write_state_function state, file_operations& file_ops)
    : file_ops_(&file_ops), file_(file), state_(std::move(state)), writing_(static_cast<bool>(state_)) {
    if (writing_) {
        buffer_offset_ = start_offset;
        open_if_exists();
    } else {
        open_file(start_offset);
    }
}

log_entry_reader::~log_entry_reader() {
    // the file is only read, so a failure of close is ignored
    if (fd_ >= 0) {
        file_ops_->close(fd_);
    }
}

void log_entry_reader::open_file(std::uint64_t start_offset) {
    fd_ = file_ops_->open(file_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        const int err = errno;
        LOG_AND_THROW_IO_EXCEPTION("cannot open file: " + file_.string(), err);
    }
    struct stat st{};
    if (file_ops_->fstat(fd_, &st) != 0) {
        const int err = errno;
        file_ops_->close(fd_);
        LOG_AND_THROW_IO_EXCEPTION("cannot get the file status: " + file_.string(), err);
    }
    file_size_ = static_cast<std::uint64_t>(st.st_size);
    if (start_offset > file_size_) {
        file_ops_->close(fd_);
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO("start offset " + std::to_string(start_offset) + " exceeds the file size "
                                            + std::to_string(file_size_) + ": " + file_.string());
    }
    buffer_offset_ = start_offset;
}

bool log_entry_reader::open_if_exists() {
    fd_ = file_ops_->open(file_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        const int err = errno;
        if (err == ENOENT) {
            return false;
        }
        LOG_AND_THROW_IO_EXCEPTION("cannot open file: " + file_.string(), err);
    }
    return true;
}

void log_entry_reader::refresh_file_size() {
    if (fd_ < 0 && ! open_if_exists()) {
        file_size_ = 0;
        return;
    }
    struct stat st{};
    if (file_ops_->fstat(fd_, &st) != 0) {
        const int err = errno;
        LOG_AND_THROW_IO_EXCEPTION("cannot get the file status: " + file_.string(), err);
    }
    file_size_ = static_cast<std::uint64_t>(st.st_size);
}

void log_entry_reader::update_write_state() {
    switch (state_()) {
    case file_write_state::writing:
        return;
    case file_write_state::failed:
        failed_ = true;
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO("writing the file failed: " + file_.string());
    case file_write_state::written:
        break;
    }
    // get the size after checking the state. In the reverse order, data whose writing completes in between would be
    // missed
    writing_ = false;
    if (fd_ < 0 && ! open_if_exists()) {
        LOG_AND_THROW_IO_EXCEPTION("cannot open file: " + file_.string(), ENOENT);
    }
    refresh_file_size();
    // if the start position or the end of the range already read exceeds the size of the completed file, the start
    // position is wrong or the file has shrunk, and the rest cannot be read
    if (buffer_offset_ + end_ > file_size_) {
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO("the read position " + std::to_string(buffer_offset_ + end_)
                                            + " exceeds the file size " + std::to_string(file_size_) + ": "
                                            + file_.string());
    }
}

void log_entry_reader::wait_and_read(std::size_t required) {
    // while the lower bound does not fit in the current buffer, wait without allocating until the rest of the file
    // reaches the lower bound
    if (required > buffer_.size()) {
        while (true) {
            refresh_file_size();
            if (required <= remaining_file_size()) {
                break;
            }
            update_write_state();
            if (! writing_) {
                return;
            }
            std::this_thread::sleep_for(wait_interval);
        }
    }
    // the lower bound fits in the current buffer, or the rest of the file has reached it. Refill without waiting, and
    // read into the whole free space
    prepare_buffer(required);
    if (read_into_buffer() == 0) {
        update_write_state();
        if (writing_) {
            std::this_thread::sleep_for(wait_interval);
        }
    }
}

log_entry_read_result log_entry_reader::next() {
    // even if the state function returns writing after a failure, do not read a rewritten file as a continuation of
    // the old buffer
    if (failed_) {
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO("writing the file failed: " + file_.string());
    }
    while (true) {
        const std::uint64_t offset = buffer_offset_ + begin_;
        const auto parsed = log_entry_view::parse(std::string_view{buffer_.data() + begin_, end_ - begin_});
        switch (parsed.result()) {
        case log_entry_parse_result::status::ok:
            begin_ += parsed.consumed();
            return log_entry_read_result::ok(parsed.view(), offset);
        case log_entry_parse_result::status::invalid_type:
            return log_entry_read_result::error(
                log_entry::read_error{log_entry::read_error::unknown_type,
                                      static_cast<log_entry::entry_type>(parsed.type_value())},
                offset);
        case log_entry_parse_result::status::short_entry:
            break;
        }
        if (writing_) {
            // being written: wait and read, then parse again. Once written, the handling below for a file not being
            // written applies
            wait_and_read(parsed.required());
            continue;
        }
        // if the lower bound exceeds the rest of the file, the rest cannot be read. Even if a length field is broken,
        // no large allocation is made
        if (parsed.required() > remaining_file_size()) {
            return end_or_short_entry();
        }
        prepare_buffer(parsed.required());
        if (read_into_buffer() == 0) {
            return end_or_short_entry();
        }
    }
}

log_entry_read_result log_entry_reader::end_or_short_entry() noexcept {
    const std::uint64_t offset = buffer_offset_ + begin_;
    if (begin_ == end_) {
        return log_entry_read_result::end(offset);
    }
    auto result = log_entry_read_result::error(
        log_entry::read_error{log_entry::read_error::short_entry,
                              static_cast<log_entry::entry_type>(static_cast<std::uint8_t>(buffer_[begin_]))},
        offset);
    // empty the buffer and move the read position to the end of the file, so that the next call returns end.
    // std::max is defensive.
    buffer_offset_ = std::max(file_size_, buffer_offset_ + end_);
    begin_ = 0;
    end_ = 0;
    return result;
}

void log_entry_reader::prepare_buffer(std::size_t required) {
    const std::size_t unparsed_size = end_ - begin_;
    // required is greater than the size of the unparsed part (the contract of parse()). If required fits in the normal
    // size, so does the unparsed part
    if (required > buffer_.size()) {
        // expand
        std::string expanded(std::max(required, buffer_size), '\0');
        std::memcpy(expanded.data(), buffer_.data() + begin_, unparsed_size);
        buffer_ = std::move(expanded);
    } else if (buffer_.size() > buffer_size && required <= buffer_size) {
        // shrink to the normal size
        std::string shrunk(buffer_size, '\0');
        std::memcpy(shrunk.data(), buffer_.data() + begin_, unparsed_size);
        buffer_ = std::move(shrunk);
    } else {
        // move to the beginning
        std::memmove(buffer_.data(), buffer_.data() + begin_, unparsed_size);
    }
    buffer_offset_ += begin_;
    begin_ = 0;
    end_ = unparsed_size;
}

std::size_t log_entry_reader::read_into_buffer() {
    while (true) {
        const ssize_t n = file_ops_->pread(fd_, buffer_.data() + end_, buffer_.size() - end_,
                                           static_cast<off_t>(buffer_offset_ + end_));
        if (n >= 0) {
            end_ += static_cast<std::size_t>(n);
            return static_cast<std::size_t>(n);
        }
        const int err = errno;
        if (err != EINTR) {
            LOG_AND_THROW_IO_EXCEPTION("cannot read file: " + file_.string(), err);
        }
    }
}

std::uint64_t log_entry_reader::remaining_file_size() const noexcept {
    const std::uint64_t position = buffer_offset_ + begin_;
    return file_size_ > position ? file_size_ - position : 0;
}

}  // namespace limestone::internal
