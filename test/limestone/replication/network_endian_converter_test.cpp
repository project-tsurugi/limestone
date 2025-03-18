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

#include "replication/network_endian_converter.h"

#include "gtest/gtest.h"
#include "limestone/api/limestone_exception.h"
#include "test_message.h"

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::limestone_exception;


// Test for receive_uint16 with an empty stream
TEST(network_endian_converter_test, receive_uint16_empty_stream) {
   std::istringstream iss("");
   try {
       network_endian_converter::receive_uint16(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       // Check that the error message contains the expected substring
       std::string expected_substring = "Failed to read uint16_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint16 with an insufficient stream (only 1 byte)
TEST(network_endian_converter_test, receive_uint16_insufficient_stream) {
   // 1 byte provided, but 2 bytes are needed for uint16_t
   std::istringstream iss("A");
   try {
    network_endian_converter::receive_uint16(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read uint16_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint32 with an empty stream
TEST(network_endian_converter_test, receive_uint32_empty_stream) {
   std::istringstream iss("");
   try {
    network_endian_converter::receive_uint32(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read uint32_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint32 with an insufficient stream (only 3 bytes)
TEST(network_endian_converter_test, receive_uint32_insufficient_stream) {
   // 3 bytes provided, but 4 bytes are needed for uint32_t
   std::istringstream iss("ABC");
   try {
    network_endian_converter::receive_uint32(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read uint32_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint64 with an empty stream
TEST(network_endian_converter_test, receive_uint64_empty_stream) {
   std::istringstream iss("");
   try {
    network_endian_converter::receive_uint64(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       // The first read (for high 32 bits) should fail
       std::string expected_substring = "Failed to read high 32 bits of uint64_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint64 with insufficient stream for the high 32 bits
TEST(network_endian_converter_test, receive_uint64_insufficient_stream_for_high) {
   // Only 3 bytes provided; 4 bytes are needed for the high 32 bits of uint64_t
   std::istringstream iss("ABC");
   try {
    network_endian_converter::receive_uint64(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read high 32 bits of uint64_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for receive_uint64 with insufficient stream for the low 32 bits
TEST(network_endian_converter_test, receive_uint64_insufficient_stream_for_low) {
   // Provide 5 bytes: 4 bytes for high 32 bits and 1 byte for low 32 bits (insufficient for low part)
   std::string data(5, 'A');
   std::istringstream iss(data);
   try {
    network_endian_converter::receive_uint64(iss);
       FAIL() << "Expected limestone_exception, but none was thrown.";
   } catch (const limestone_exception& ex) {
       std::string expected_substring = "Failed to read low 32 bits of uint64_t value from stream";
       EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
           << "Error message was: " << ex.what();
   }
}

// Test for byte order conversion with streams
TEST(network_endian_converter_test, byte_order_conversion_with_streams) {
    uint16_t host16 = 0x1234;  // 16-bit value
    uint32_t host32 = 0x12345678;  // 32-bit value
    uint64_t host64 = 0x1234567890ABCDEF;  // 64-bit value

    // Create a stringstream to simulate a stream
    std::ostringstream oss;
    
    // Write 16-bit, 32-bit, and 64-bit values to the stream
    network_endian_converter::send_uint16(oss, host16);
    network_endian_converter::send_uint32(oss, host32);
    network_endian_converter::send_uint64(oss, host64);
    
    // Create an input stream from the serialized data
    std::istringstream iss(oss.str());
    
    // Read 16-bit, 32-bit, and 64-bit values from the stream
    uint16_t network16 = network_endian_converter::receive_uint16(iss);
    uint32_t network32 = network_endian_converter::receive_uint32(iss);
    uint64_t network64 = network_endian_converter::receive_uint64(iss);
    
    // Verify that the deserialized values match the original values
    EXPECT_EQ(network16, host16);
    EXPECT_EQ(network32, host32);
    EXPECT_EQ(network64, host64);
}

}  // namespace limestone::testing