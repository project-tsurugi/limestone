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

#include "gtest/gtest.h"
#include "limestone/api/limestone_exception.h"
#include "replication/socket_io.h"

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::limestone_exception;


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


}  // namespace limestone::testing