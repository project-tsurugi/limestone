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

#include <replication/message_rdma_finalize.h>

#include <replication/message_error.h>
#include <replication/message_rdma_finalize_ack.h>
#include <replication/replication_message_io.h>
#include "control_channel_handler_resources.h"

namespace limestone::replication {

message_type_id message_rdma_finalize::get_message_type_id() const {
    return message_type_id::RDMA_FINALIZE;
}

void message_rdma_finalize::send_body(replication_message_io& /*io*/) const {
    // Empty body: phase-transition signal only.
}

void message_rdma_finalize::receive_body(replication_message_io& /*io*/) {
    // Empty body: phase-transition signal only.
}

void message_rdma_finalize::post_receive(handler_resources& resources) {
    auto* control_resources = dynamic_cast<control_channel_handler_resources*>(&resources);
    if (control_resources == nullptr) {
        message_error err;
        err.set_error(message_error::rdma_finalize_error_invalid_resources,
            "Invalid handler resources for RDMA_FINALIZE");
        replication_message::send(resources.get_replication_message_io(), err);
        resources.get_replication_message_io().flush();
        return;
    }

    auto& server = control_resources->get_server();
    auto finalize_result = server.finalize_rdma();
    if (finalize_result == replica_server::rdma_finalize_result::not_initialized) {
        message_error err;
        err.set_error(message_error::rdma_finalize_error_not_initialized,
            "RDMA stack is not initialized");
        replication_message::send(resources.get_replication_message_io(), err);
        resources.get_replication_message_io().flush();
        return;
    }
    if (finalize_result == replica_server::rdma_finalize_result::failed) {
        message_error err;
        err.set_error(message_error::rdma_finalize_error_finalize_failed,
            "Failed to finalize RDMA channel setup");
        replication_message::send(resources.get_replication_message_io(), err);
        resources.get_replication_message_io().flush();
        return;
    }

    message_rdma_finalize_ack ack{};
    replication_message::send(resources.get_replication_message_io(), ack);
    resources.get_replication_message_io().flush();
}

std::unique_ptr<replication_message> message_rdma_finalize::create() {
    return std::make_unique<message_rdma_finalize>();
}

}  // namespace limestone::replication
