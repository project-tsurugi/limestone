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
 #include "test_message.h"  // Include the test_message class
 #include "limestone/api/limestone_exception.h"
 
 namespace limestone::testing {
 
 using namespace limestone::replication;
 using limestone::api::limestone_exception;
 
// Test for creating a message using the message type ID
TEST(replication_message_test, create_message_with_valid_type_id) {
    // Create a stream and write type information and message data
    std::ostringstream oss;
    message_type_id type_id = message_type_id::TESTING;
    oss.write(reinterpret_cast<const char*>(&type_id), sizeof(type_id));  // Write the message type ID
    oss << "Test Message Data";  // Write some dummy message data
    
    std::istringstream iss(oss.str());  // Create an input stream from the serialized data
    // Deserialize the message
    auto message = replication_message::receive(iss);
    
    // Verify that the created message is of the expected type
    EXPECT_EQ(message->get_message_type_id(), limestone::replication::message_type_id::TESTING);
}
 
// Test for invalid message type ID (should throw exception)
TEST(replication_message_test, create_message_with_invalid_type_id) {
    // Create a stream and write invalid type information (e.g., type_id = 999)
    std::ostringstream oss;
    message_type_id invalid_type_id = static_cast<message_type_id>(999);  // Invalid type ID
    oss.write(reinterpret_cast<const char*>(&invalid_type_id), sizeof(invalid_type_id));  // Write invalid type ID
    oss << "Invalid Test Message Data";  // Write some dummy message data
    
    std::istringstream iss(oss.str());  // Create an input stream from the serialized data
    
    
    // Expect the runtime_error exception with the expected message
    try {
        auto message = replication_message::receive(iss);
        FAIL() << "Expected std::runtime_error, but no exception was thrown.";
    } catch (const limestone_exception& e) {
        // Verify that the exception message starts with the expected string
        EXPECT_TRUE(std::string(e.what()).find("Unknown message type ID") == 0)
            << "Expected exception message to start with 'Unknown message type ID'.";
    }
}


 
 // Test for send and receive with type information
 TEST(replication_message_test, serialize_and_deserialize_message) {
     test_message msg;
 
     // Serialize message to stringstream (or other suitable stream) with type info
     std::ostringstream oss;
     replication_message::send(oss, msg);  // Send with type info
     std::string serialized_data = oss.str();
 
     // Deserialize message from stringstream
     std::istringstream iss(serialized_data);
     auto deserialized_msg = replication_message::receive(iss);  // Receive with type info

     // Verify that the deserialized message type matches the original message
     EXPECT_EQ(deserialized_msg->get_message_type_id(), msg.get_message_type_id());
 }
 
 // Test for message creation using the factory function
 TEST(replication_message_test, create_message_using_factory) {
     auto message = test_message::create();
 
     // Verify that the created message is of the expected type
     EXPECT_EQ(message->get_message_type_id(), limestone::replication::message_type_id::TESTING);
 }
 

// Test protected methods send_body and receive_body using derived class
TEST(replication_message_test, send_body_receive_body) {
    // Create a test message instance
    test_message msg;

    // Prepare an output stream to serialize the message
    std::ostringstream oss;
    msg.send_body(oss);  // Call the protected send_body method

    // Prepare an input stream to deserialize the message
    std::istringstream iss(oss.str());
    test_message deserialized_msg;
    deserialized_msg.receive_body(iss);  // Call the protected receive_body method

    // Verify that the deserialized message data matches the original message
    EXPECT_EQ(deserialized_msg.get_message_type_id(), msg.get_message_type_id());
}

 // Test for byte order conversion with streams
 TEST(replication_message_test, byte_order_conversion_with_streams) {
     uint16_t host16 = 0x1234;  // 16-bit value
     uint32_t host32 = 0x12345678;  // 32-bit value
     uint64_t host64 = 0x1234567890ABCDEF;  // 64-bit value
 
     // Create a stringstream to simulate a stream
     std::ostringstream oss;
     
     // Write 16-bit, 32-bit, and 64-bit values to the stream
     replication_message::send_uint16(oss, host16);
     replication_message::send_uint32(oss, host32);
     replication_message::send_uint64(oss, host64);
     
     // Create an input stream from the serialized data
     std::istringstream iss(oss.str());
     
     // Read 16-bit, 32-bit, and 64-bit values from the stream
     uint16_t network16 = replication_message::receive_uint16(iss);
     uint32_t network32 = replication_message::receive_uint32(iss);
     uint64_t network64 = replication_message::receive_uint64(iss);
     
     // Verify that the deserialized values match the original values
     EXPECT_EQ(network16, host16);
     EXPECT_EQ(network32, host32);
     EXPECT_EQ(network64, host64);
 }

// Test for incomplete stream with 0 bytes
TEST(replication_message_test, incomplete_stream_0_bytes) {
    std::istringstream iss("");
    try {
        auto message = replication_message::receive(iss);
        FAIL() << "Expected limestone_io_exception, but none was thrown.";
    } catch (const limestone_io_exception& ex) {
        // Check that the error message contains the expected substring
        std::string expected_substring = "Failed to read uint16_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for incomplete stream with 1 byte (only part of type information)
TEST(replication_message_test, incomplete_stream_1_byte) {
    std::istringstream iss("A");
    try {
        auto message = replication_message::receive(iss);
        FAIL() << "Expected limestone_io_exception, but none was thrown.";
    } catch (const limestone_io_exception& ex) {
        std::cerr << ex.what() << std::endl;
        std::string expected_substring = "Failed to read uint16_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for incomplete stream with 2 bytes (type information exists but no message body)
TEST(replication_message_test, incomplete_stream_2_bytes) {
    std::ostringstream oss;
    message_type_id type_id = message_type_id::TESTING;
    oss.write(reinterpret_cast<const char*>(&type_id), sizeof(type_id));  
    std::istringstream iss(oss.str()); 
    auto message = replication_message::receive(iss);
    EXPECT_EQ(message->get_message_type_id(), limestone::replication::message_type_id::TESTING);    
}

// Test for receive_uint16 with an empty stream
TEST(replication_message_test, receive_uint16_empty_stream) {
    std::istringstream iss("");
    try {
        replication_message::receive_uint16(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        // Check that the error message contains the expected substring
        std::string expected_substring = "Failed to read uint16_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint16 with an insufficient stream (only 1 byte)
TEST(replication_message_test, receive_uint16_insufficient_stream) {
    // 1 byte provided, but 2 bytes are needed for uint16_t
    std::istringstream iss("A");
    try {
        replication_message::receive_uint16(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint16_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint32 with an empty stream
TEST(replication_message_test, receive_uint32_empty_stream) {
    std::istringstream iss("");
    try {
        replication_message::receive_uint32(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint32_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint32 with an insufficient stream (only 3 bytes)
TEST(replication_message_test, receive_uint32_insufficient_stream) {
    // 3 bytes provided, but 4 bytes are needed for uint32_t
    std::istringstream iss("ABC");
    try {
        replication_message::receive_uint32(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read uint32_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint64 with an empty stream
TEST(replication_message_test, receive_uint64_empty_stream) {
    std::istringstream iss("");
    try {
        replication_message::receive_uint64(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        // The first read (for high 32 bits) should fail
        std::string expected_substring = "Failed to read high 32 bits of uint64_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint64 with insufficient stream for the high 32 bits
TEST(replication_message_test, receive_uint64_insufficient_stream_for_high) {
    // Only 3 bytes provided; 4 bytes are needed for the high 32 bits of uint64_t
    std::istringstream iss("ABC");
    try {
        replication_message::receive_uint64(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read high 32 bits of uint64_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

// Test for receive_uint64 with insufficient stream for the low 32 bits
TEST(replication_message_test, receive_uint64_insufficient_stream_for_low) {
    // Provide 5 bytes: 4 bytes for high 32 bits and 1 byte for low 32 bits (insufficient for low part)
    std::string data(5, 'A');
    std::istringstream iss(data);
    try {
        replication_message::receive_uint64(iss);
        FAIL() << "Expected limestone_exception, but none was thrown.";
    } catch (const limestone_exception& ex) {
        std::string expected_substring = "Failed to read low 32 bits of uint64_t value from stream";
        EXPECT_NE(std::string(ex.what()).find(expected_substring), std::string::npos)
            << "Error message was: " << ex.what();
    }
}

 }  // namespace limestone::testing
 