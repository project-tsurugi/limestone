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

#include <replication/message_rdma_finalize_ack.h>

#include <replication/replication_message_io.h>

namespace limestone::replication {

message_type_id message_rdma_finalize_ack::get_message_type_id() const {
    return message_type_id::RDMA_FINALIZE_ACK;
}

void message_rdma_finalize_ack::send_body(replication_message_io& /*io*/) const {
    // Empty body: phase-transition signal only.
}

void message_rdma_finalize_ack::receive_body(replication_message_io& /*io*/) {
    // Empty body: phase-transition signal only.
}

std::unique_ptr<replication_message> message_rdma_finalize_ack::create() {
    return std::make_unique<message_rdma_finalize_ack>();
}

}  // namespace limestone::replication
