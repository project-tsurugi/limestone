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

#include <iostream>
#include <memory>
#include <unordered_map>

#include "../limestone_exception_helper.h"

namespace limestone::replication {

// Enum to define message types
enum class message_type_id : uint16_t {
    TESTING = 0,  // for testing purposes only
    SESSION_BEGIN = 1,
    SESSION_END = 2,
    GROUP_COMMIT = 3,
    LOG_ENTRY = 4
};

// Abstract class representing a replication message
class replication_message {
public:
    virtual ~replication_message() = default;

    // Get the message type ID (to be implemented by derived classes)
    virtual message_type_id get_message_type_id() const = 0;

    // Method to serialize and deserialize with type information
    static void send(std::ostream& os, const replication_message& message);
    static std::unique_ptr<replication_message> receive(std::istream& is);

    // Create message object based on type ID (factory method)
    static std::unique_ptr<replication_message> create_message(message_type_id type);

    // Process the message after it has been received.
    virtual void post_receive() = 0;

    // Retrieve internal data for testing purposes.
    virtual std::string get_data_for_testing() const = 0;

    // Register message type with its factory function
    static void register_message_type(message_type_id type, std::unique_ptr<replication_message> (*factory)());

    // Write to the stream with byte order conversion
    static inline void send_uint16(std::ostream& os, uint16_t value) {
        value = htons(value);  // Convert to network byte order
        os.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    static inline void send_uint32(std::ostream& os, uint32_t value) {
        value = htonl(value);  // Convert to network byte order
        os.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    static inline void send_uint64(std::ostream& os, uint64_t value) {
        uint32_t high = htonl(static_cast<uint32_t>(value >> 32));  // Convert high 32 bits to network byte order
        uint32_t low = htonl(static_cast<uint32_t>(value & 0xFFFFFFFF));  // Convert low 32 bits to network byte order
        os.write(reinterpret_cast<const char*>(&high), sizeof(high));
        os.write(reinterpret_cast<const char*>(&low), sizeof(low));
    }

    // Read from the stream with byte order conversion and error checking
    static inline uint16_t receive_uint16(std::istream& is) {
        uint16_t value;
        is.read(reinterpret_cast<char*>(&value), sizeof(value));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t value from stream", errno);
        }
        return ntohs(value);  // Convert from network byte order
    }

    static inline uint32_t receive_uint32(std::istream& is) {
        uint32_t value;
        is.read(reinterpret_cast<char*>(&value), sizeof(value));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t value from stream", errno);
        }
        return ntohl(value);  // Convert from network byte order
    }

    static inline uint64_t receive_uint64(std::istream& is) {
        uint32_t high, low;
        is.read(reinterpret_cast<char*>(&high), sizeof(high));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t value from stream", errno);
        }
        is.read(reinterpret_cast<char*>(&low), sizeof(low));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t value from stream", errno);
        }
        uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32) | ntohl(low);  // Convert to host byte order
        return value;
    }


 protected:
     // This function is used to write the type information along with the message data
     static void write_type_info(std::ostream& os, message_type_id type_id);
     static message_type_id read_type_info(std::istream& is);
 
     // send_body and receive_body should not be accessed externally, so make them protected
     // These are only intended to be overridden by derived classes
     virtual void send_body(std::ostream& os) const = 0;
     virtual void receive_body(std::istream& is) = 0;
 
 

 private:
     // Static map to hold message type factories
     static std::unordered_map<message_type_id, std::unique_ptr<replication_message> (*)()>& get_message_map();
 };
 
 }  // namespace limestone::replication
 
 
 