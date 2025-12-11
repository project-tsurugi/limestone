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

#include <replication/message_error.h>
#include <replication/message_rdma_init_ack.h>
#include <replication/socket_io.h>
#include "control_channel_handler_resources.h"

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

void message_rdma_init::post_receive(handler_resources& resources) {
    auto* control_resources = dynamic_cast<control_channel_handler_resources*>(&resources);
    if (control_resources == nullptr) {
        message_error err;
        err.set_error(message_error::rdma_init_error_invalid_resources,
            "Invalid handler resources for RDMA_INIT");
        replication_message::send(resources.get_socket_io(), err);
        resources.get_socket_io().flush();
        return;
    }

    auto& server = control_resources->get_server();
    auto init_result = server.initialize_rdma_receiver(slot_count_);
    if (init_result == replica_server::rdma_init_result::already_initialized) {
        message_error err;
        err.set_error(message_error::rdma_init_error_already_initialized,
            "RDMA receiver already initialized");
        replication_message::send(resources.get_socket_io(), err);
        resources.get_socket_io().flush();
        return;
    }
    if (init_result == replica_server::rdma_init_result::failed) {
        message_error err;
        err.set_error(message_error::rdma_init_error_init_failed,
            "Failed to initialize RDMA receiver");
        replication_message::send(resources.get_socket_io(), err);
        resources.get_socket_io().flush();
        return;
    }

    auto remote_dma_address = server.get_rdma_dma_address();
    if (! remote_dma_address.has_value()) {
        message_error err;
        err.set_error(message_error::rdma_init_error_no_dma_address,
            "RDMA receiver did not expose DMA address");
        replication_message::send(resources.get_socket_io(), err);
        resources.get_socket_io().flush();
        return;
    }

    message_rdma_init_ack ack{remote_dma_address.value()};
    replication_message::send(resources.get_socket_io(), ack);
    resources.get_socket_io().flush();
}

std::unique_ptr<replication_message> message_rdma_init::create() {
    return std::make_unique<message_rdma_init>(0);
}

}  // namespace limestone::replication
