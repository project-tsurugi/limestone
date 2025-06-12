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

#include "replication/message_error.h"
#include "socket_io.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

void message_error::set_error(uint16_t error_code, std::string error_message) {
    error_code_ = error_code;
    error_message_ = std::move(error_message);
}

message_type_id message_error::get_message_type_id() const {
    return message_type_id::COMMON_ERROR;
}

void message_error::send_body(socket_io& io) const {
    io.send_uint8(static_cast<uint8_t>(response_type::RESPONSE_TYPE_ERROR));
    io.send_uint16(error_code_);
    io.send_string(error_message_);
}

void message_error::receive_body(socket_io& io) {
    uint8_t resp = io.receive_uint8();
    if (resp != static_cast<uint8_t>(response_type::RESPONSE_TYPE_ERROR)) {
        LOG_AND_THROW_EXCEPTION("Invalid response_type for message_error");
    }
    error_code_ = io.receive_uint16();
    error_message_ = io.receive_string();
}

std::unique_ptr<replication_message> message_error::create() {
    return std::make_unique<message_error>();
}

} // namespace limestone::replication
