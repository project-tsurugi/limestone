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
#include <cstring>
#include <memory>

#include "limestone_exception_helper.h"
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
    uint16_t net_value = htons(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint32(uint32_t value) {
    uint32_t net_value = htonl(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint64(uint64_t value) {
    constexpr uint64_t mask32 = 0xFFFFFFFFULL;
    uint32_t high = htonl(static_cast<uint32_t>(value >> 32U));
    uint32_t low  = htonl(static_cast<uint32_t>(value & mask32));
    std::array<char, sizeof(high) + sizeof(low)> buffer{};
    std::memcpy(buffer.data(), &high, sizeof(high));
    std::memcpy(buffer.data() + sizeof(high), &low, sizeof(low));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint8(uint8_t value) {
    std::array<char, sizeof(value)> buffer{};
    std::memcpy(buffer.data(), &value, sizeof(value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_string(const std::string &value) {
    send_uint32(static_cast<uint32_t>(value.size()));  // TODO: Check for overflow
    out_stream_->write(value.data(), static_cast<std::streamsize>(value.size()));
}

uint16_t socket_io::receive_uint16() {
    std::array<char, sizeof(uint16_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t from input stream", errno);
    }
    uint16_t net_value = 0;
    std::memcpy(&net_value, buffer.data(), sizeof(net_value));
    return ntohs(net_value);
}

uint32_t socket_io::receive_uint32() {
    std::array<char, sizeof(uint32_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t from input stream", errno);
    }
    uint32_t net_value = 0;
    std::memcpy(&net_value, buffer.data(), sizeof(net_value));
    return ntohl(net_value);
}

uint64_t socket_io::receive_uint64() {
    uint32_t high = 0;
    uint32_t low = 0;
    {
        std::array<char, sizeof(uint32_t)> buffer{};
        in_stream_->read(buffer.data(), buffer.size());
        if (!(*in_stream_)) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t from input stream", errno);
        }
        std::memcpy(&high, buffer.data(), sizeof(uint32_t));
    }
    {
        std::array<char, sizeof(uint32_t)> buffer{};
        in_stream_->read(buffer.data(), buffer.size());
        if (!(*in_stream_)) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t from input stream", errno);
        }
        std::memcpy(&low, buffer.data(), sizeof(uint32_t));
    }
    uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32U)
                     | static_cast<uint64_t>(ntohl(low));
    return value;
}

uint8_t socket_io::receive_uint8() {
    std::array<char, sizeof(uint8_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint8_t from input stream", errno);
    }
    uint8_t value = 0;
    std::memcpy(&value, buffer.data(), sizeof(value));
    return value;
}

std::string socket_io::receive_string() {
    uint32_t len = receive_uint32();
    std::string result;
    result.resize(len);
    in_stream_->read(result.data(), static_cast<std::streamsize>(result.size()));
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read string body from input stream", errno);
    }
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
    return out_stream_->str();
}

void socket_io::close() {
    flush();
    if (!is_string_mode_) {
        if (socket_fd_ != -1) {
            int ret = 0;
            do {
                ret = ::close(socket_fd_);
            } while (ret == -1 && errno == EINTR);
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

}  // namespace limestone::replication
