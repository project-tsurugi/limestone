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

 #include "session_begin_message.h"

 namespace limestone::replication {
 
 // Send the actual session begin message data
 void session_begin_message::send_body(std::ostream& os) const {
     os << "Session Begin Data";  // Implement actual serialization logic here
 }
 
 // Deserialize the session begin message data
 void session_begin_message::receive_body(std::istream& is) {
     std::string data;
     is >> data;
     // Implement actual deserialization logic here
 }
 
 message_type_id session_begin_message::get_message_type_id() const {
     return message_type_id::SESSION_BEGIN;
 }
 
 std::shared_ptr<replication_message> session_begin_message::create() {
     return std::make_shared<session_begin_message>();
 }
 
 }  // namespace limestone::replication
 