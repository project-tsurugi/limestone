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
#include <string_view>

#include "socket_streambuf.h"
#include <limestone/api/blob_id_type.h>

namespace limestone::replication {

using limestone::api::blob_id_type;

class socket_io {
public:
    // Constructor for real socket mode: initialize with a valid socket file descriptor.
    explicit socket_io(int fd);

    // Constructor for string mode: initializes both input and output streams using the provided string.
    // for testing purposes.
    explicit socket_io(const std::string &initial);

    virtual ~socket_io();

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

    /**
     * @brief Returns the current byte size of the output buffer without copying its contents.
     * @return Number of bytes currently accumulated in the output buffer.
     */
    [[nodiscard]] std::size_t get_out_size() const;

    // Close the socket file descriptor (real mode) or clear the input and output streams (string mode).
    void close();

    // Check if the end of the input stream has been reached.
    [[nodiscard]] bool eof();

    /**
     * @brief Returns true if there is at least one more byte available to read
     *        in the input stream (string mode only; always false in socket mode).
     *        Use this to loop over multiple messages packed in a single RDMA payload.
     * @return true if more data can be read without blocking.
     */
    [[nodiscard]] bool has_unread_data() const;

    /**
     * @brief Retrieve the underlying socket file descriptor.
     * @return socket file descriptor in real socket mode; -1 in string mode.
     */
    [[nodiscard]] int get_socket_fd() const noexcept;

    /**
     * @brief Reset output buffer while retaining allocated capacity.
     */
    void reset_output_buffer();

    /**
     * @brief Send a blob file over the channel.
     * @param blob_id ID of the blob to send.
     */
    virtual void send_blob(blob_id_type blob_id);

    /**
     * @brief Receive a blob from the channel and write it to the datastore.
     * @return The blob ID of the received blob.
     */
    virtual blob_id_type receive_blob();

protected:
    [[nodiscard]] std::ostream& get_out_stream(); 
    [[nodiscard]] std::istream& get_in_stream();
private:
    [[nodiscard]] bool wait_for_writable() const;

    /**
     * @brief Read exactly the requested number of bytes from the input stream.
     * @param buffer Destination buffer.
     * @param size Number of bytes to read.
     * @param description Human-readable value description used in diagnostics.
     * @throws limestone::api::limestone_io_exception if the stream cannot provide
     *         exactly @p size bytes.
     */
    void read_exact(char* buffer, std::streamsize size, std::string_view description);

    bool is_string_mode_;  // true: string mode, false: real socket mode.
    int socket_fd_;        // Valid in real socket mode; -1 in string mode.

    std::unique_ptr<std::istream> in_stream_;
    std::unique_ptr<socket_streambuf> socket_buf_;
    std::unique_ptr<std::ostringstream> out_stream_;
 };
 
 } // namespace limestone::replication
 
