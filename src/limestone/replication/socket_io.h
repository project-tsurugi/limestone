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
 */

 #pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <istream>
#include <streambuf>

#include "socket_streambuf.h"

namespace limestone::replication {

class socket_io {
public:
    // Constructor for real socket mode: initialize with a valid socket file descriptor.
    explicit socket_io(int fd);

    // Constructor for string mode: initializes both input and output streams using the provided string.
    // for testing purposes.
    explicit socket_io(const std::string &initial);

    ~socket_io();

    // Deleted copy constructor and assignment operator to prevent copying.
    socket_io(const socket_io &) = delete;
    socket_io &operator=(const socket_io &) = delete;
    socket_io(socket_io &&) noexcept = default;
    socket_io &operator=(socket_io &&) noexcept = default;

    /**
     * Sends raw binary data over the socket.
     *
     * NOTE: This method includes logic to handle partial writes and EAGAIN/EWOULDBLOCK errors
     * using poll() for compatibility with unit tests using non-blocking sockets.
     *
     * However, socket_io is designed primarily for use with blocking sockets.
     * Do NOT use this method in production with non-blocking sockets, as its non-blocking
     * behavior is intended solely for unit testing purposes and is not fully configurable
     * (e.g., timeout values are fixed, EINTR handling is not extensively tested).
     */
    [[nodiscard]] bool send_raw(const std::string &data) const;

    // Send methods: serialize the given value and write it to the internal output buffer.
    void send_uint16(uint16_t value);
    void send_uint32(uint32_t value);
    void send_uint64(uint64_t value);
    void send_uint8(uint8_t value);
    void send_string(const std::string &value);

    // Receive methods: read data from the input stream and convert to host byte order.
    [[nodiscard]] uint16_t receive_uint16();
    [[nodiscard]] uint32_t receive_uint32();
    [[nodiscard]] uint64_t receive_uint64();
    [[nodiscard]] uint8_t receive_uint8();
    [[nodiscard]] std::string receive_string();

    // Flush method: in real socket mode, flush the output buffer by sending its content via send_raw().
    // In string mode, flush() does nothing.
    bool flush();

    // Getters for the output buffer contents
    [[nodiscard]] std::string get_out_string() const;

    // Close the socket file descriptor (real mode) or clear the input and output streams (string mode).
    void close();

protected:
    [[nodiscard]] std::ostream& get_out_stream(); 
    [[nodiscard]] std::istream& get_in_stream();
private:
    [[nodiscard]] bool wait_for_writable() const;

    bool is_string_mode_;  // true: string mode, false: real socket mode.
    int socket_fd_;        // Valid in real socket mode; -1 in string mode.

    std::unique_ptr<std::istream> in_stream_;
    std::unique_ptr<socket_streambuf> socket_buf_;
    std::unique_ptr<std::ostringstream> out_stream_;
 };
 
 } // namespace limestone::replication
 