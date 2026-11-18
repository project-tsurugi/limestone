/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include "replication/message_compaction_boundary.h"

#include "control_channel_handler_resources.h"
#include "datastore_impl.h"
#include "limestone_exception_helper.h"
#include "replication_message_io.h"

namespace limestone::replication {

message_compaction_boundary::message_compaction_boundary(std::uint64_t boundary_epoch, std::uint64_t blob_id_boundary)
    : boundary_epoch_(boundary_epoch), blob_id_boundary_(blob_id_boundary) {}

message_type_id message_compaction_boundary::get_message_type_id() const {
    return message_type_id::COMPACTION_BOUNDARY;
}

void message_compaction_boundary::send_body(replication_message_io& io) const {
    io.send_uint64(boundary_epoch_);
    io.send_uint64(blob_id_boundary_);
}

void message_compaction_boundary::receive_body(replication_message_io& io) {
    boundary_epoch_ = io.receive_uint64();
    blob_id_boundary_ = io.receive_uint64();
}

std::unique_ptr<replication_message> message_compaction_boundary::create() {
    return std::make_unique<message_compaction_boundary>();
}

std::uint64_t message_compaction_boundary::boundary_epoch() const {
    return boundary_epoch_;
}

std::uint64_t message_compaction_boundary::blob_id_boundary() const {
    return blob_id_boundary_;
}

void message_compaction_boundary::post_receive(handler_resources& resources) {
    TRACE_START << "boundary_epoch: " << boundary_epoch_ << ", blob_id_boundary: " << blob_id_boundary_;
    auto& cch_resources = dynamic_cast<control_channel_handler_resources&>(resources);
    cch_resources.get_datastore().get_impl()->set_received_compaction_boundary({boundary_epoch_, blob_id_boundary_});
    TRACE_END;
}

}  // namespace limestone::replication
