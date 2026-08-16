/*
 * Copyright 2022-2025 Project Tsurugi.
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
 * 
 */

#include "replication_message_io.h"

#include <poll.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <sstream>

#include "limestone_exception_helper.h"
#include "primitive_wire_codec.h"
namespace limestone::replication {

namespace {

[[nodiscard]] std::uint32_t checked_string_size(std::size_t size) {
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        LOG_AND_THROW_EXCEPTION("Replication string payload is too large");
    }
    return static_cast<std::uint32_t>(size);
}

}  // namespace

// Constructor for real socket mode.
replication_message_io::replication_message_io(int fd)
    : is_string_mode_(false),
      socket_fd_(fd),
      socket_buf_(std::make_unique<socket_streambuf>(fd))
{
    // Create std::istream using the raw pointer from socket_buf_.
    in_stream_ = std::make_unique<std::istream>(socket_buf_.get());
}

// Constructor for string mode.
replication_message_io::replication_message_io(const std::string &initial)
    : is_string_mode_(true),
      socket_fd_(-1),
      socket_buf_(nullptr)
{
    // Create std::istringstream from the initial string.
    in_stream_ = std::make_unique<std::istringstream>(initial);
}

replication_message_io::~replication_message_io() {
    close();
}


bool replication_message_io::wait_for_writable() const {
    struct pollfd pfd{ socket_fd_, POLLOUT, 0 };
    while (true) {
        int ret = poll(&pfd, 1, 10000);
        if (ret < 0) {
            if (errno == EINTR) continue;
            LOG_LP(ERROR) << "poll() failed: " << strerror(errno);
            return false;
        }
        if (ret == 0) {
            LOG_LP(ERROR) << "poll() timed out: socket not writable";
            return false;
        }
        return true;
    }
}

bool replication_message_io::send_raw(const std::string &data) const {
    TRACE_START;
    std::string_view buffer{data};

    while (!buffer.empty()) {
        ssize_t sent = ::send(socket_fd_, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        if (sent >= 0) {
            buffer.remove_prefix(static_cast<size_t>(sent));
            continue;
        }

        int err = errno;
        if (err == EINTR) {
            continue;
        }

        if (err == EAGAIN || err == EWOULDBLOCK) {
            if (!wait_for_writable()) {
                return false;
            }
            continue;
        }

        LOG_LP(ERROR) << "send() failed: " << strerror(err);
        return false;
    }
    TRACE_END;
    return true;
}

void replication_message_io::send_uint16(uint16_t value) {
    auto buffer = primitive_wire_codec::encode_uint16(value);
    out_buffer_.append(buffer.data(), buffer.size());
}

void replication_message_io::send_uint32(uint32_t value) {
    auto buffer = primitive_wire_codec::encode_uint32(value);
    out_buffer_.append(buffer.data(), buffer.size());
}

void replication_message_io::send_uint64(uint64_t value) {
    auto buffer = primitive_wire_codec::encode_uint64(value);
    out_buffer_.append(buffer.data(), buffer.size());
}

void replication_message_io::send_uint8(uint8_t value) {
    auto buffer = primitive_wire_codec::encode_uint8(value);
    out_buffer_.append(buffer.data(), buffer.size());
}

void replication_message_io::send_string(const std::string &value) {
    send_uint32(checked_string_size(value.size()));
    out_buffer_.append(value.data(), value.size());
}

void replication_message_io::read_exact(char* buffer, std::streamsize size, std::string_view description) {
    in_stream_->read(buffer, size);
    if (*in_stream_) {
        return;
    }

    auto const bytes_read = in_stream_->gcount();
    std::ostringstream message;
    message << "Failed to read " << description << " from input stream"
            << " (requested=" << size
            << ", got=" << bytes_read
            << ", eof=" << in_stream_->eof()
            << ", fail=" << in_stream_->fail()
            << ", bad=" << in_stream_->bad() << ")";

    LOG_AND_THROW_IO_EXCEPTION(message.str(), EIO);
}

uint16_t replication_message_io::receive_uint16() {
    std::array<char, sizeof(uint16_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint16_t");
    return primitive_wire_codec::decode_uint16(std::string_view{buffer.data(), buffer.size()});
}

uint32_t replication_message_io::receive_uint32() {
    std::array<char, sizeof(uint32_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint32_t");
    return primitive_wire_codec::decode_uint32(std::string_view{buffer.data(), buffer.size()});
}

uint64_t replication_message_io::receive_uint64() {
    std::array<char, sizeof(uint64_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(sizeof(uint32_t)), "high 32 bits of uint64_t");
    read_exact(
            std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(uint32_t))),
            static_cast<std::streamsize>(sizeof(uint32_t)),
            "low 32 bits of uint64_t");
    return primitive_wire_codec::decode_uint64(std::string_view{buffer.data(), buffer.size()});
}

uint8_t replication_message_io::receive_uint8() {
    std::array<char, sizeof(uint8_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint8_t");
    return primitive_wire_codec::decode_uint8(std::string_view{buffer.data(), buffer.size()});
}

std::string replication_message_io::receive_string() {
    uint32_t len = receive_uint32();
    std::string result;
    result.resize(len);
    read_exact(result.data(), static_cast<std::streamsize>(result.size()), "string body");
    return result;
}

bool replication_message_io::flush() {
    TRACE_START;
    if (is_string_mode_) {
        in_stream_ = std::make_unique<std::istringstream>(out_buffer_);
        return true;
    }
    if (out_buffer_.empty()) {
        return true;
    }
    bool ret = send_raw(out_buffer_);
    out_buffer_.clear();
    TRACE_END << "ret = " << ret;
    return ret;
}

std::string replication_message_io::get_out_string() const {
    return out_buffer_;
}

std::string_view replication_message_io::get_out_view() const noexcept {
    return out_buffer_;
}

std::size_t replication_message_io::get_out_size() const {
    return out_buffer_.size();
}

bool replication_message_io::has_unread_data() const {
    if (!is_string_mode_) {
        return false;
    }
    if (! in_stream_) {
        return false;
    }
    return in_stream_->peek() != std::char_traits<char>::eof();
}

void replication_message_io::reset_output_buffer() {
    out_buffer_.clear();
}

void replication_message_io::close() {
    // in_stream_ is null only on a moved-from object. Skip closing there: the
    // default move copies socket_fd_ without resetting the source, so closing
    // from both objects would double-close the descriptor.
    if (! in_stream_) {
        return;
    }
    flush();
    if (!is_string_mode_) {
        if (socket_fd_ != -1) {
            int ret = ::close(socket_fd_);
            while (ret == -1 && errno == EINTR) {
                ret = ::close(socket_fd_);
            }
            if (ret == -1 && errno != EBADF) {
                LOG_LP(WARNING) << "close() failed: " << strerror(errno);
            }
            socket_fd_ = -1;
        }
    }
}

void replication_message_io::write_out_bytes(char const* data, std::size_t size) {
    out_buffer_.append(data, size);
}

std::istream& replication_message_io::get_in_stream() {
    return *in_stream_;
}

bool replication_message_io::eof() {
    return in_stream_->eof();
}

int replication_message_io::get_socket_fd() const noexcept {
    return socket_fd_;
}

void replication_message_io::send_blob(blob_id_type /*blob_id*/) {
    LOG_LP(FATAL) << "send_blob called on base replication_message_io: blob-capable IO class required";
}

blob_id_type replication_message_io::receive_blob() {
    LOG_LP(FATAL) << "receive_blob called on base replication_message_io: blob-capable IO class required";
}

}  // namespace limestone::replication
