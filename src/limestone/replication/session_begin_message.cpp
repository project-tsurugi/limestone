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

#include "socket_io.h"

namespace limestone::replication {

void session_begin_message::set_param(std::string configuration_id, uint64_t epoch_number) {
    configuration_id_ = std::move(configuration_id);
    epoch_number_ = epoch_number;
 }

 // Send the actual session begin message data
 void session_begin_message::send_body(socket_io& io) const {
    io.send_uint8(connection_type_);
    io.send_uint64(protocol_version_);
    io.send_string(configuration_id_);
    io.send_uint64(epoch_number_);
 }

 // Deserialize the session begin message data
 void session_begin_message::receive_body(socket_io& io) {
     connection_type_ = io.receive_uint8();
     protocol_version_ = io.receive_uint64();
     configuration_id_ = io.receive_string();
     epoch_number_ = io.receive_uint64();
 }

 message_type_id session_begin_message::get_message_type_id() const {
     return message_type_id::SESSION_BEGIN;
 }
 
 std::unique_ptr<replication_message> session_begin_message::create() {
     return std::make_unique<session_begin_message>();
 }
 
 }  // namespace limestone::replication
 