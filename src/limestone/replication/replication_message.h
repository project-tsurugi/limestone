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

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "limestone_exception_helper.h"
#include "socket_io.h"

namespace limestone::replication {

enum class message_type_id : uint16_t {
    // Control‑channel requests
    SESSION_BEGIN = 100,
    SESSION_END = 101,
    GROUP_COMMIT = 102,
    GC_BOUNDARY_SWITCH = 103,

    // Log‑channel requests
    LOG_CHANNEL_CREATE = 200,
    LOG_ENTRY = 201,

    // Responses
    SESSION_BEGIN_ACK = 300,
    COMMON_ACK = 301,
    COMMON_ERROR = 302,

    // For testing only
    TESTING = 9999
};

// Enum to define connection types
enum connection_type : uint8_t {
    CONNECTION_TYPE_CONTROL_CHANNEL = 0,
    CONNECTION_TYPE_LOG_CHANNEL = 1
};

// Response discriminator for replication responses
enum class response_type : uint8_t {
    RESPONSE_TYPE_ACK = 0,
    RESPONSE_TYPE_ERROR = 1
};

constexpr uint64_t protocol_version = 1;

// Abstract class representing a replication message
class replication_message {
public:
    virtual ~replication_message() = default;

    replication_message(const replication_message&) = delete;
    replication_message& operator=(const replication_message&) = delete;
    replication_message(replication_message&&) = delete;
    replication_message& operator=(replication_message&&) = delete;


    // Get the message type ID (to be implemented by derived classes)
    [[nodiscard]] virtual message_type_id get_message_type_id() const = 0;

    // Method to serialize and deserialize with type information
    static void send(socket_io& io, const replication_message& message);
    static std::unique_ptr<replication_message> receive(socket_io& io);

    // Create message object based on type ID (factory method)
    [[nodiscard]] static std::unique_ptr<replication_message> create_message(message_type_id type);

    // Process the message after it has been received.
    virtual void post_receive();

    // Register message type with its factory function
    static void register_message_type(message_type_id type, std::unique_ptr<replication_message> (*factory)());

 
 protected:
     replication_message() = default;
     // This function is used to write the type information along with the message data
     static void write_type_info(socket_io& io, message_type_id type_id);
 
     // send_body and receive_body should not be accessed externally, so make them protected
     // These are only intended to be overridden by derived classes
     virtual void send_body(socket_io& io) const = 0;
     virtual void receive_body(socket_io& io) = 0;
 
 

 private:
     // Static map to hold message type factories
     static std::unordered_map<message_type_id, std::unique_ptr<replication_message> (*)()>& get_message_map();
 };
 
 }  // namespace limestone::replication
 
 
 