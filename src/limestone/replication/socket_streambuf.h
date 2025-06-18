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

 #pragma once

#include <streambuf>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>

namespace limestone::replication {

// Custom streambuf to wrap a socket file descriptor.
class socket_streambuf : public std::streambuf {
public:
    // Constructor: initialize with a valid socket file descriptor.
    explicit socket_streambuf(int socket_fd);

protected:
    /**
     * Overrides underflow() to refill the stream buffer by reading from the socket.
     *
     * NOTE: This implementation handles non-blocking mode (EAGAIN/EWOULDBLOCK)
     * using poll(), specifically to support unit tests. However, socket_streambuf
     * is designed to be used primarily in blocking mode.
     *
     * Non-blocking mode support in this method is provided strictly for unit testing,
     * and is not fully robust or configurable. Do NOT rely on non-blocking behavior
     * in production code, as it has limited testing and fixed timeout parameters.
     */
    int_type underflow() override;

private:
    [[nodiscard]] bool wait_for_readable() const;

    int socket_fd_;
    std::vector<char> buffer_;
};

} // namespace limestone::replication
