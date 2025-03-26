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


#include "replication/message_gc_boundary_switch.h"
#include "socket_io.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

message_gc_boundary_switch::message_gc_boundary_switch(uint16_t write_version)
    : write_version_(write_version) {}

message_type_id message_gc_boundary_switch::get_message_type_id() const {
    return message_type_id::GC_BOUNDARY_SWITCH;
}

void message_gc_boundary_switch::send_body(socket_io& io) const {
    io.send_uint16(write_version_);
}

void message_gc_boundary_switch::receive_body(socket_io& io) {
    write_version_ = io.receive_uint16();
}

void message_gc_boundary_switch::post_receive() {}

std::unique_ptr<replication_message> message_gc_boundary_switch::create() {
    return std::make_unique<message_gc_boundary_switch>();
}

uint16_t message_gc_boundary_switch::write_version() const {
    return write_version_;
}

} // namespace limestone::replication
