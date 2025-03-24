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


 #include "replication/message_session_begin_ack.h"
 #include "socket_io.h"
 #include "limestone_exception_helper.h"
 
 namespace limestone::replication {
 
 void message_session_begin_ack::set_session_secret(std::string session_secret) {
     session_secret_ = std::move(session_secret);
 }
 
 message_type_id message_session_begin_ack::get_message_type_id() const {
     return message_type_id::SESSION_BEGIN_ACK;
 }
 
 void message_session_begin_ack::send_body(socket_io& io) const {
     io.send_uint8(static_cast<uint8_t>(response_type::RESPONSE_TYPE_ACK));
     io.send_string(session_secret_);
 }
 
 void message_session_begin_ack::receive_body(socket_io& io) {
     uint8_t resp = io.receive_uint8();
     if (resp != static_cast<uint8_t>(response_type::RESPONSE_TYPE_ACK)) {
         LOG_AND_THROW_EXCEPTION("Invalid response_type for session_begin_ack");
     }
     session_secret_ = io.receive_string();
 }
 
 std::unique_ptr<replication_message> message_session_begin_ack::create() {
     return std::make_unique<message_session_begin_ack>();
 }
 
 } // namespace limestone::replication
 