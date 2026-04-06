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

#include <replication/message_rdma_init_ack.h>

#include <replication/socket_io.h>

namespace limestone::replication {

message_type_id message_rdma_init_ack::get_message_type_id() const {
    return message_type_id::RDMA_INIT_ACK;
}

void message_rdma_init_ack::send_body(socket_io& io) const {
    io.send_uint64(remote_dma_address_);
}

void message_rdma_init_ack::receive_body(socket_io& io) {
    remote_dma_address_ = io.receive_uint64();
}

std::unique_ptr<replication_message> message_rdma_init_ack::create() {
    return std::make_unique<message_rdma_init_ack>();
}

}  // namespace limestone::replication

