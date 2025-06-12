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

 #include "replication/message_log_channel_create.h"
 #include "socket_io.h"
 #include "limestone_exception_helper.h"
 
 namespace limestone::replication {
 
 void message_log_channel_create::set_secret(std::string secret) {
     secret_ = std::move(secret);
 }
 
 message_type_id message_log_channel_create::get_message_type_id() const {
     return message_type_id::LOG_CHANNEL_CREATE;
 }
 
 void message_log_channel_create::send_body(socket_io& io) const {
     io.send_uint8(connection_type_);
     io.send_string(secret_);
 }
 
 void message_log_channel_create::receive_body(socket_io& io) {
     connection_type_ = io.receive_uint8();
     if (connection_type_ != CONNECTION_TYPE_LOG_CHANNEL) {
         LOG_AND_THROW_EXCEPTION("Invalid connection_type for message_log_channel_create");
     }
     secret_ = io.receive_string();
 }
 
 std::unique_ptr<replication_message> message_log_channel_create::create() {
     return std::make_unique<message_log_channel_create>();
 }
 
 }  // namespace limestone::replication
 
 