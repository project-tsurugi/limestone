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

#include "replication/message_group_commit.h"

#include "control_channel_handler_resources.h"
#include "limestone_exception_helper.h"
#include "replication/message_ack.h"
#include "socket_io.h"
namespace limestone::replication {

message_group_commit::message_group_commit(uint64_t epoch_number)
    : epoch_number_(epoch_number) {}

message_type_id message_group_commit::get_message_type_id() const {
    return message_type_id::GROUP_COMMIT;
}

void message_group_commit::send_body(socket_io& io) const {
    io.send_uint64(epoch_number_);
}

void message_group_commit::receive_body(socket_io& io) {
    epoch_number_ = io.receive_uint64();
}

std::unique_ptr<replication_message> message_group_commit::create() {
    return std::make_unique<message_group_commit>();
}

uint64_t message_group_commit::epoch_number() const {
    return epoch_number_;
}

void message_group_commit::post_receive(handler_resources& resources) {
    TRACE_START << "epoch_number: " << epoch_number_;
    // Check if the epoch number is valid
    // switch the epoch in the datastore
    auto& cch_resources = dynamic_cast<control_channel_handler_resources&>(resources);
    auto& datastore = cch_resources.get_datastore();
    datastore.persist_and_propagate_epoch_id(epoch_number_);

    // Send acknowledgment
    message_ack ack;
    socket_io& io = cch_resources.get_socket_io();
    replication_message::send(io, ack);
    io.flush();
    TRACE_END;
}

}  // namespace limestone::replication
