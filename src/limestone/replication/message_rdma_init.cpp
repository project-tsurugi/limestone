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

#include <replication/message_rdma_init.h>

#include <replication/socket_io.h>

namespace limestone::replication {

message_rdma_init::message_rdma_init(uint32_t slot_count) : slot_count_(slot_count) {}

message_type_id message_rdma_init::get_message_type_id() const {
    return message_type_id::RDMA_INIT;
}

void message_rdma_init::send_body(socket_io& io) const {
    io.send_uint32(slot_count_);
}

void message_rdma_init::receive_body(socket_io& io) {
    slot_count_ = io.receive_uint32();
}

void message_rdma_init::post_receive(handler_resources& /*resources*/) {
    // TODO: Implement RDMA initialization handling on replica side.
}

std::unique_ptr<replication_message> message_rdma_init::create() {
    return std::make_unique<message_rdma_init>(0);
}

}  // namespace limestone::replication
