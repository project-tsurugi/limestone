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

#include "socket_io.h"

#include <poll.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <memory>
#include <sstream>

#include "limestone_exception_helper.h"
#include "primitive_wire_codec.h"
namespace limestone::replication {

// Constructor for real socket mode.
socket_io::socket_io(int fd)
    : is_string_mode_(false),
      socket_fd_(fd),
      socket_buf_(std::make_unique<socket_streambuf>(fd)),
      out_stream_(std::make_unique<std::ostringstream>(std::ios_base::out))
{
    // Create std::istream using the raw pointer from socket_buf_.
    in_stream_ = std::make_unique<std::istream>(socket_buf_.get());
}

// Constructor for string mode.
socket_io::socket_io(const std::string &initial)
    : is_string_mode_(true),
      socket_fd_(-1),
      socket_buf_(nullptr),
      out_stream_(std::make_unique<std::ostringstream>(std::ios_base::out))
{
    // Create std::istringstream from the initial string.
    in_stream_ = std::unique_ptr<std::istream>(new std::istringstream(initial) );
}

socket_io::~socket_io() {
    close();
}


bool socket_io::wait_for_writable() const {
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

bool socket_io::send_raw(const std::string &data) const {
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

void socket_io::send_uint16(uint16_t value) {
    auto buffer = primitive_wire_codec::encode_uint16(value);
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint32(uint32_t value) {
    auto buffer = primitive_wire_codec::encode_uint32(value);
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint64(uint64_t value) {
    auto buffer = primitive_wire_codec::encode_uint64(value);
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint8(uint8_t value) {
    auto buffer = primitive_wire_codec::encode_uint8(value);
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_string(const std::string &value) {
    send_uint32(static_cast<uint32_t>(value.size()));  // TODO: Check for overflow
    out_stream_->write(value.data(), static_cast<std::streamsize>(value.size()));
}

void socket_io::read_exact(char* buffer, std::streamsize size, std::string_view description) {
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

uint16_t socket_io::receive_uint16() {
    std::array<char, sizeof(uint16_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint16_t");
    return primitive_wire_codec::decode_uint16(std::string_view{buffer.data(), buffer.size()});
}

uint32_t socket_io::receive_uint32() {
    std::array<char, sizeof(uint32_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint32_t");
    return primitive_wire_codec::decode_uint32(std::string_view{buffer.data(), buffer.size()});
}

uint64_t socket_io::receive_uint64() {
    std::array<char, sizeof(uint64_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(sizeof(uint32_t)), "high 32 bits of uint64_t");
    read_exact(
            std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(uint32_t))),
            static_cast<std::streamsize>(sizeof(uint32_t)),
            "low 32 bits of uint64_t");
    return primitive_wire_codec::decode_uint64(std::string_view{buffer.data(), buffer.size()});
}

uint8_t socket_io::receive_uint8() {
    std::array<char, sizeof(uint8_t)> buffer{};
    read_exact(buffer.data(), static_cast<std::streamsize>(buffer.size()), "uint8_t");
    return primitive_wire_codec::decode_uint8(std::string_view{buffer.data(), buffer.size()});
}

std::string socket_io::receive_string() {
    uint32_t len = receive_uint32();
    std::string result;
    result.resize(len);
    read_exact(result.data(), static_cast<std::streamsize>(result.size()), "string body");
    return result;
}

bool socket_io::flush() {
    TRACE_START;
    if (is_string_mode_) {
        std::string data = out_stream_->str();
        in_stream_ = std::make_unique<std::istringstream>(data);
        return true;
    }
    std::string data = out_stream_->str();
    if (data.empty()) {
        return true;
    }
    bool ret = send_raw(data);
    out_stream_->str("");
    out_stream_->clear();
    TRACE_END << "ret = " << ret;
    return ret;
}

std::string socket_io::get_out_string() const {
    if (! out_stream_) {
        return std::string{};
    }
    return out_stream_->str();
}

std::size_t socket_io::get_out_size() const {
    if (! out_stream_) {
        return 0;
    }
    std::ostream::pos_type pos = out_stream_->tellp();
    if (pos == std::ostream::pos_type(-1)) {
        return 0;
    }
    return static_cast<std::size_t>(pos);
}

bool socket_io::has_unread_data() const {
    if (!is_string_mode_) {
        return false;
    }
    if (! in_stream_) {
        return false;
    }
    return in_stream_->peek() != std::char_traits<char>::eof();
}

void socket_io::reset_output_buffer() {
    if (! out_stream_) {
        return;
    }
    out_stream_->str(std::string{});
    out_stream_->clear();
}

void socket_io::close() {
    if (! out_stream_) {
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

std::ostream& socket_io::get_out_stream() {
    return *out_stream_;
}

std::istream& socket_io::get_in_stream() {
    return *in_stream_;
}

bool socket_io::eof() {
    return in_stream_->eof();
}

int socket_io::get_socket_fd() const noexcept {
    return socket_fd_;
}

void socket_io::send_blob(blob_id_type /*blob_id*/) {
    LOG_LP(FATAL) << "send_blob called on base socket_io: blob-capable IO class required";
}

blob_id_type socket_io::receive_blob() {
    LOG_LP(FATAL) << "receive_blob called on base socket_io: blob-capable IO class required";
}

}  // namespace limestone::replication
