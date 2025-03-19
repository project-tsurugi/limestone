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

#include "replication_message.h"
#include "socket_io.h"
#include "../limestone_exception_helper.h"

 namespace limestone::replication {
 
 // Static map to hold message type factories
 std::unordered_map<message_type_id, std::unique_ptr<replication_message> (*)()>& replication_message::get_message_map() {
     static std::unordered_map<message_type_id, std::unique_ptr<replication_message> (*)()> map;
     return map;
 }
 
 // Register message type with its factory function
 void replication_message::register_message_type(message_type_id type, std::unique_ptr<replication_message> (*factory)()) {
     get_message_map()[type] = factory;
 }
 
 // Send method with type information (cannot be overridden)
 void replication_message::send(std::ostream& os, const replication_message& message) {
     message_type_id type_id = message.get_message_type_id();  // Get the message type ID
     write_type_info(os, type_id);  // Send type information first
     message.send_body(os);  // Call the derived class's send method to send the actual data
 }
 
 // Receive method with type information (cannot be overridden)
 std::unique_ptr<replication_message> replication_message::receive(std::istream& is) {
    // Read the message type ID from the stream with error checking and byte order conversion
    message_type_id type_id = read_type_info(is);
    
    // Create the message using the appropriate factory function.
    // The factory function now returns a std::unique_ptr<replication_message>
    std::unique_ptr<replication_message> message = create_message(type_id);
    
    // Deserialize the message body using the derived class's implementation
    message->receive_body(is);
    
    // Return the newly created message object
    return message;
}
 
 // Write type information to the stream
 void replication_message::write_type_info(std::ostream& os, message_type_id type_id) {
    socket_io::send_uint16(os, static_cast<uint16_t>(type_id));  
 }
 
 // Read type information from the stream
 message_type_id replication_message::read_type_info(std::istream& is) {
    uint16_t value = socket_io::receive_uint16(is);
    return static_cast<message_type_id>(value);
 }
 
 // Create message object based on type ID
 std::unique_ptr<replication_message> replication_message::create_message(message_type_id type) {
     auto it = get_message_map().find(type);
     if (it != get_message_map().end()) {
         return it->second();  // Call the factory function to create the message
     }
     LOG_AND_THROW_EXCEPTION("Unknown message type ID");
 }
 
 }  // namespace limestone::replication
 