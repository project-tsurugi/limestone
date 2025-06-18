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
#include "replication/socket_io.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <random>
#include <thread>

#include "gtest/gtest.h"
#include "limestone/api/limestone_exception.h"
#include "replication/socket_io.h"

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::limestone_exception;

// Test subclass for socket_io to allow access to protected methods
class testable_socket_io : public limestone::replication::socket_io {
public:
    using limestone::replication::socket_io::get_in_stream;  
    using limestone::replication::socket_io::socket_io;     
};

// Test for receive_uint16 with an empty stream
TEST(socket_io_test, receive_uint16_empty_stream) {
    socket_io io("");
    try {
        [[maybe_unused]] uint16_t value = io.receive_uint16();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        // Check that the error message contains the expected substring
        std::string expected_substring = "Failed to read uint16_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
    }
}

// Test for receive_uint16 with an insufficient stream (only 1 byte)
TEST(socket_io_test, receive_uint16_insufficient_stream) {
    // 1 byte provided, but 2 bytes are needed for uint16_t
    socket_io io("A");
    try {
        [[maybe_unused]] uint16_t value = io.receive_uint16();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint16_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
    }
}

// Test for receive_uint32 with an empty stream
TEST(socket_io_test, receive_uint32_empty_stream) {
    socket_io io("");
    try {
        [[maybe_unused]] uint16_t value = io.receive_uint32();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint32_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
    }
}

// Test for receive_uint32 with an insufficient stream (only 3 bytes)
TEST(socket_io_test, receive_uint32_insufficient_stream) {
   // 3 bytes provided, but 4 bytes are needed for uint32_t
   socket_io io("ABC");
   try {
    [[maybe_unused]] uint32_t value = io.receive_uint32();
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read uint32_t from input stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
   }
}

// Test for receive_uint64 with an empty stream
TEST(socket_io_test, receive_uint64_empty_stream) {
    socket_io io("");
    try {
        [[maybe_unused]] uint64_t value = io.receive_uint64();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        // The first read (for high 32 bits) should fail
        std::string expected_substring = "Failed to read high 32 bits of uint64_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
    }
}

// Test for receive_uint64 with insufficient stream for the high 32 bits
TEST(socket_io_test, receive_uint64_insufficient_stream_for_high) {
   // Only 3 bytes provided; 4 bytes are needed for the high 32 bits of uint64_t
   socket_io io("ABC");
   try {
       [[maybe_unused]] uint64_t value = io.receive_uint64();
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read high 32 bits of uint64_t from input stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
   }
}

// Test for receive_uint64 with insufficient stream for the low 32 bits
TEST(socket_io_test, receive_uint64_insufficient_stream_for_low) {
    // Provide 5 bytes: 4 bytes for high 32 bits and 1 byte for low 32 bits (insufficient for low part)
    std::string data(5, 'A');
    socket_io io(data);
    try {
        [[maybe_unused]] uint64_t value = io.receive_uint64();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read low 32 bits of uint64_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos) << "Error message was: " << ex.what();
    }
}

// Test for byte order conversion using socket_io's get_out_string() and round-trip functionality.
TEST(socket_io_test, byte_order_conversion_with_streams) {
    // Define host values.
    uint8_t host8 = 0x12;                   // 8-bit value.
    uint16_t host16 = 0x1234;               // 16-bit value.
    uint32_t host32 = 0x12345678;           // 32-bit value.
    uint64_t host64 = 0x1234567890ABCDEF;    // 64-bit value.
    std::string host_string = "Hello, World!"; // String value.

    // Create a socket_io instance in string mode.
    socket_io io_send("");
    
    // Write values to the internal output buffer.
    io_send.send_uint8(host8);
    io_send.send_uint16(host16);
    io_send.send_uint32(host32);
    io_send.send_uint64(host64);
    io_send.send_string(host_string);
    
    // Flush the output buffer to finalize writing.
    ASSERT_TRUE(io_send.flush());
    
    // Retrieve the raw output data from the output buffer.
    std::string out_data = io_send.get_out_string();
    size_t pos = 0;
    
    // --- Verify each value in network byte order ---
    
    // Verify uint8_t: 8-bit value is identical in network order.
    uint8_t net8 = 0;
    std::memcpy(&net8, out_data.data() + pos, sizeof(net8));
    pos += sizeof(net8);
    EXPECT_EQ(net8, host8);
    
    // Verify uint16_t: the value should equal htons(host16).
    uint16_t net16 = 0;
    std::memcpy(&net16, out_data.data() + pos, sizeof(net16));
    pos += sizeof(net16);
    EXPECT_EQ(net16, htons(host16));
    
    // Verify uint32_t: the value should equal htonl(host32).
    uint32_t net32 = 0;
    std::memcpy(&net32, out_data.data() + pos, sizeof(net32));
    pos += sizeof(net32);
    EXPECT_EQ(net32, htonl(host32));
    
    // Verify uint64_t: written as two 32-bit parts.
    uint32_t high = 0, low = 0;
    std::memcpy(&high, out_data.data() + pos, sizeof(high));
    pos += sizeof(high);
    std::memcpy(&low, out_data.data() + pos, sizeof(low));
    pos += sizeof(low);
    uint64_t net64 = (static_cast<uint64_t>(high) << 32U) | low;
    uint64_t expected_net64 = (static_cast<uint64_t>(htonl(static_cast<uint32_t>(host64 >> 32))) << 32U)
                                | htonl(static_cast<uint32_t>(host64));
    EXPECT_EQ(net64, expected_net64);
    
    // Verify string: first 4 bytes indicate the string length (uint32_t) in network order.
    uint32_t net_strlen = 0;
    std::memcpy(&net_strlen, out_data.data() + pos, sizeof(net_strlen));
    pos += sizeof(net_strlen);
    EXPECT_EQ(net_strlen, htonl(static_cast<uint32_t>(host_string.size())));
    
    // Then, the remaining bytes should match the string body.
    std::string out_string_body(out_data.begin() + pos, out_data.end());
    EXPECT_EQ(out_string_body, host_string);
    
    // --- Round-trip test: use the output data as input for a new socket_io instance ---
    socket_io io_receive(out_data);
    uint8_t rec8 = io_receive.receive_uint8();
    uint16_t rec16 = io_receive.receive_uint16();
    uint32_t rec32 = io_receive.receive_uint32();
    uint64_t rec64 = io_receive.receive_uint64();
    std::string rec_string = io_receive.receive_string();
    
    EXPECT_EQ(rec8, host8);
    EXPECT_EQ(rec16, host16);
    EXPECT_EQ(rec32, host32);
    EXPECT_EQ(rec64, host64);
    EXPECT_EQ(rec_string, host_string);
}


// Test for receive_uint8 with an empty stream.
TEST(socket_io_test, receive_uint8_empty_stream) {
    // Create a socket_io in string mode with an empty input.
    socket_io io("");
    try {
        [[maybe_unused]] uint8_t value = io.receive_uint8();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception &ex) {
        std::string expected = "Failed to read uint8_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_string with an empty stream (i.e. missing length).
TEST(socket_io_test, receive_string_empty_stream) {
    socket_io io("");
    try {
        [[maybe_unused]] std::string s = io.receive_string();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception &ex) {
        // receive_string() first calls receive_uint32().
        std::string expected = "Failed to read uint32_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_string with insufficient body data.
TEST(socket_io_test, receive_string_insufficient_body) {
    // Prepare a socket_io in string mode with only the length field (5) but no body.
    socket_io io_send("");
    io_send.send_uint32(5); // length = 5, but no subsequent string data.
    ASSERT_TRUE(io_send.flush());
    std::string out_data = io_send.get_out_string();
    
    // Create a new socket_io for receiving using the generated output.
    socket_io io_receive(out_data);
    try {
        [[maybe_unused]] std::string s = io_receive.receive_string();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception &ex) {
        std::string expected = "Failed to read string body from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for string round-trip (binary data).
TEST(socket_io_test, string_round_trip) {
    std::string original = std::string("Hello\0World", 11);
    socket_io io_send("");
    io_send.send_string(original);
    ASSERT_TRUE(io_send.flush());
    std::string out_data = io_send.get_out_string();
    
    // Use the output data as the input for a new socket_io.
    socket_io io_receive(out_data);
    std::string result = io_receive.receive_string();
    EXPECT_EQ(result, original);
}

// Test for round-trip of an empty string.
TEST(socket_io_test, string_round_trip_empty) {
    std::string original = "";
    socket_io io_send("");
    io_send.send_string(original);
    ASSERT_TRUE(io_send.flush());
    std::string out_data = io_send.get_out_string();
    
    socket_io io_receive(out_data);
    std::string result = io_receive.receive_string();
    EXPECT_EQ(result.size(), 0u);
    EXPECT_EQ(result, original);
}

// Utility function to create and bind a listening socket.
int create_server_socket(uint16_t port) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(listen_fd, -1) << "Failed to create socket: " << strerror(errno);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    int reuse = 1;
    EXPECT_EQ(::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)), 0)
        << "Failed to set SO_REUSEADDR: " << strerror(errno);

    EXPECT_EQ(::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0)
        << "Failed to bind: " << strerror(errno);

    EXPECT_EQ(::listen(listen_fd, 1), 0)
        << "Failed to listen: " << strerror(errno);

    return listen_fd;
}

// Test for socket-based communication (round-trip test)
TEST(socket_io_test, socket_round_trip) {
    const uint16_t test_port = 12345;
    const std::string test_message = "Test socket_io message";

    int listen_fd = create_server_socket(test_port);

    // Start server thread to accept a connection and read data
    std::thread server_thread([&]() {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int conn_fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        EXPECT_NE(conn_fd, -1) << "Failed to accept connection: " << strerror(errno);

        socket_io server_io(conn_fd);
        std::string received_message = server_io.receive_string();
        EXPECT_EQ(received_message, test_message);

        server_io.send_string("ACK");
        server_io.flush();

        server_io.close();
    });

    // Give server a moment to start listening
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Client-side socket creation and connection
    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(client_fd, -1) << "Failed to create client socket: " << strerror(errno);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server_addr.sin_port = htons(test_port);

    EXPECT_EQ(::connect(client_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)), 0)
        << "Failed to connect to server: " << strerror(errno);

    socket_io client_io(client_fd);
    client_io.send_string(test_message);
    EXPECT_TRUE(client_io.flush());

    std::string reply = client_io.receive_string();
    EXPECT_EQ(reply, "ACK");

    client_io.close();

    server_thread.join();
    ::close(listen_fd);
}

TEST(socket_io_test, socket_round_trip_large_nonblocking) {
    const uint16_t test_port = 12345;

    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_NE(listen_fd, -1);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server_addr.sin_port = htons(test_port);

    int reuse = 1;
    ASSERT_EQ(::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)), 0);
    ASSERT_EQ(::bind(listen_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)), 0);
    ASSERT_EQ(::listen(listen_fd, 1), 0);

    // Start server thread
    std::thread server_thread([&]() {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int conn_fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        ASSERT_NE(conn_fd, -1);

        socket_io server_io(conn_fd);
        std::string received_message = server_io.receive_string();
        server_io.send_string(received_message);
        ASSERT_TRUE(server_io.flush());

        server_io.close();
    });

    // Client setup
    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_NE(client_fd, -1);

    // Set socket to non-blocking mode
    int flags = fcntl(client_fd, F_GETFL, 0);
    ASSERT_NE(flags, -1);
    ASSERT_EQ(fcntl(client_fd, F_SETFL, flags | O_NONBLOCK), 0);

    int connect_ret = ::connect(client_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr));
    if (connect_ret == -1 && errno != EINPROGRESS) {
        FAIL() << "connect failed: " << strerror(errno);
    }

    // Retrieve send buffer size
    int sndbuf_size = 0;
    socklen_t optlen = sizeof(sndbuf_size);
    ASSERT_EQ(::getsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, &optlen), 0);

    const size_t large_message_size = static_cast<size_t>(sndbuf_size) * 10;

    // Generate large binary data quickly (each 4-byte chunk has a unique value)
    std::string large_message;
    large_message.resize(large_message_size);
    
    uint32_t* data_ptr = reinterpret_cast<uint32_t*>(large_message.data());
    size_t num_chunks = large_message_size / sizeof(uint32_t);
    
    for (size_t i = 0; i < num_chunks; ++i) {
        data_ptr[i] = static_cast<uint32_t>(i);
    }
    
    // Handle remaining bytes if any
    size_t remaining_bytes = large_message_size % sizeof(uint32_t);
    if (remaining_bytes) {
        char* byte_ptr = large_message.data() + num_chunks * sizeof(uint32_t);
        for (size_t i = 0; i < remaining_bytes; ++i) {
            byte_ptr[i] = static_cast<char>(i & 0xFF);
        }
    }

    socket_io client_io(client_fd);

    // The first call to send() implicitly waits until connection established or fails
    client_io.send_string(large_message);
    ASSERT_TRUE(client_io.flush());

    // Now receive response
    std::string reply = client_io.receive_string();
    EXPECT_EQ(reply, large_message);

    client_io.close();
    server_thread.join();
    ::close(listen_fd);
}

// Test for EOF on an empty stream
TEST(socket_io_test, eof_empty_stream) {
    testable_socket_io io("");
    EXPECT_FALSE(io.eof());
    // Attempt to read data from the stream
    char buffer;
    io.get_in_stream().read(&buffer, 1);

    // Now check for EOF after attempting to read
    EXPECT_TRUE(io.eof()) << "Expected EOF, but the stream was not at EOF.";
}

// Test for not EOF when data is available in the stream
TEST(socket_io_test, eof_data_available) {
    socket_io io("Test data");
    EXPECT_FALSE(io.eof()) << "Expected not EOF, but the stream reached EOF prematurely.";
}

// Test for EOF when the stream ends before receiving expected data
TEST(socket_io_test, eof_incomplete_data) {
    socket_io io("A");  // Only one byte, expecting two bytes for uint16_t
    try {
        [[maybe_unused]] uint16_t value = io.receive_uint16();
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint16_t from input stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }

    // Now check for EOF since there was not enough data
    EXPECT_TRUE(io.eof()) << "Expected EOF after incomplete data, but the stream was not at EOF.";
}

// Test for EOF in string mode after the stream is closed
TEST(socket_io_test, eof_after_close_string_mode) {
    testable_socket_io io("AAA");
    io.close();  // Close the stream

    // Attempt to read data from the stream
    char buffer;
    io.get_in_stream().read(&buffer, 1);

    // Check that EOF is not true in string mode after closing the stream
    EXPECT_TRUE(io.eof()) << "Expected EOF after stream close in socket mode, but it was not EOF.";
}

// Test for EOF after the stream is closed in socket mode
TEST(socket_io_test, eof_after_close_socket_mode) {
    // Create a socket in a server-client setup or mock it
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(server_fd, -1);

    testable_socket_io io(server_fd);
    io.close();  // Close the socket

    // Attempt to read data from the stream
    char buffer;
    io.get_in_stream().read(&buffer, 1);

    // Check that EOF is true after closing the stream in socket mode
    EXPECT_TRUE(io.eof()) << "Expected EOF after stream close in socket mode, but it was not EOF.";
}

}  // namespace limestone::testing