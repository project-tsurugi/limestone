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

#include "socket_streambuf.h"

#include <poll.h>
#include <sys/socket.h>

#include <cstddef>   // for std::ptrdiff_t
#include <iterator>  // for std::next

#include "limestone_exception_helper.h"

namespace limestone::replication {

socket_streambuf::socket_streambuf(int socket_fd) : socket_fd_(socket_fd) {
    // Retrieve the socket's receive buffer size.
    int rcv_buf_size = 65536;  // Fallback default: 64KB.
    socklen_t optlen = sizeof(rcv_buf_size);
    if (getsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &rcv_buf_size, &optlen) != 0) {
        LOG_LP(WARNING) << "getsockopt(SO_RCVBUF) failed: " << strerror(errno)
                        << ", using default 1024 bytes.";
    }
    // Resize the buffer to the received size.
    buffer_.resize(static_cast<std::size_t>(rcv_buf_size));
    // Use std::next with an explicit cast to avoid narrowing conversion.
    auto* buf_begin = buffer_.data();
    auto* buf_end = std::next(buf_begin, static_cast<std::ptrdiff_t>(buffer_.size()));
    // Initialize get area as empty, but with allocated capacity.
    setg(buf_begin, buf_end, buf_end);
}

bool socket_streambuf::wait_for_readable() const {
    pollfd pfd{socket_fd_, POLLIN, 0};
    while (true) {
        int ret = poll(&pfd, 1, 10000);
        if (ret > 0) return true;
        if (ret == 0) {
            LOG_LP(ERROR) << "poll() timed out: socket not readable";
            return false;
        }
        if (errno == EINTR) continue;
        LOG_LP(ERROR) << "poll() failed: " << strerror(errno);
        return false;
    }
}

std::streambuf::int_type socket_streambuf::underflow() {
    if (gptr() < egptr()) {
        return traits_type::to_int_type(*gptr());
    }

    ssize_t bytes_read = 0;
    while ((bytes_read = ::recv(socket_fd_, buffer_.data(), buffer_.size(), 0)) <= 0) {
        if (bytes_read == 0) {
            return traits_type::eof();  // connection closed
        }
        int err = errno;
        if (err == EINTR) {
            continue;
        }
        if ((err == EAGAIN || err == EWOULDBLOCK) && wait_for_readable()) {
            continue;
        }
        LOG_LP(ERROR) << "recv() failed: " << strerror(err);
        return traits_type::eof();
    }

    setg(buffer_.data(), buffer_.data(), std::next(buffer_.data(), bytes_read));
    return traits_type::to_int_type(*gptr());
}

}  // namespace limestone::replication
