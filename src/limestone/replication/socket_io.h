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

    // Write raw data to the socket (real mode) or to the output stream (string mode) using MSG_NOSIGNAL.
    bool write_raw(const std::string &data) const;

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

    // Flush method: in real socket mode, flush the output buffer by sending its content via write_raw().
    // In string mode, flush() does nothing.
    bool flush();

    // Getters for the output buffer contents
    std::string get_out_string() const;

    // Close the socket file descriptor (real mode) or clear the input and output streams (string mode).
    void close();

private:
    bool is_string_mode_;  // true: string mode, false: real socket mode.
    int socket_fd_;        // Valid in real socket mode; -1 in string mode.

    std::unique_ptr<std::istream> in_stream_;
    std::unique_ptr<socket_streambuf> socket_buf_;
    std::unique_ptr<std::ostringstream> out_stream_;
 };
 
 } // namespace limestone::replication
 