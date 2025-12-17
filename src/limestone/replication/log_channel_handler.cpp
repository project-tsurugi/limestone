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

#include "log_channel_handler.h"

#include "replication_message.h"
#include "message_error.h"
#include "message_log_channel_create.h"
#include "message_log_entries.h"
#include "validation_result.h"
#include "socket_io.h"
#include "logging_helper.h"
#include <glog/logging.h>
#include <rdma_comm/rdma_receiver.h>
#include <rdma_comm/rdma_frame_header.h>
#include <rdma_comm/ack_message.h>
#include <vector>

namespace limestone::replication {

log_channel_handler::log_channel_handler(replica_server &server, socket_io& io) noexcept
    : channel_handler_base(server, io){}

validation_result log_channel_handler::validate_initial(std::unique_ptr<replication_message> request) {
    if (request->get_message_type_id() != message_type_id::LOG_CHANNEL_CREATE) {
        std::ostringstream msg;
        msg << "Invalid message type: " << static_cast<int>(request->get_message_type_id()) << ", expected LOG_CHANNEL_CREATE";
        return validation_result::error(
            message_error::log_channel_error_invalid_type, msg.str());
    }

    auto *msg = dynamic_cast<message_log_channel_create*>(request.get());
    if (!msg) {
        return validation_result::error(
            message_error::log_channel_error_bad_cast,
            "Failed to cast to message_log_channel_create");
    }

    // TODO その他のバリデーション処理を入れる

    auto& server = get_server();
    auto& ds = get_server().get_datastore();    
    log_channel_ = &ds.create_channel(server.get_location());
    

    // Perform additional validation as needed
    return validation_result::success();
}

void log_channel_handler::send_initial_ack() const {
    send_ack();
}

void log_channel_handler::handle_rdma_data_event(
    rdma::communication::rdma_receive_data_event const& event) {
    auto const& header = event.header;
    if (header.version != rdma::communication::rdma_frame_protocol_version) {
        LOG_LP(ERROR) << "RDMA frame version mismatch: expected "
                      << static_cast<int>(rdma::communication::rdma_frame_protocol_version)
                      << " got " << static_cast<int>(header.version);
        pending_rdma_frames_.clear();
        return;
    }

    if (header.payload_size != event.payload.size()) {
        LOG_LP(ERROR) << "RDMA payload size mismatch: header=" << header.payload_size
                      << " actual=" << event.payload.size();
        pending_rdma_frames_.clear();
        return;
    }

    if (header.sequence_number != next_sequence_number_) {
        LOG_LP(ERROR) << "RDMA sequence mismatch: expected=" << next_sequence_number_
                      << " received=" << header.sequence_number;
        // TODO: recover or request retransmission instead of simple drop.
        next_sequence_number_ = static_cast<std::uint16_t>(header.sequence_number + 1);
        pending_rdma_frames_.clear();
        return;
    }
    pending_rdma_frames_.push_back(event);
    next_sequence_number_ = static_cast<std::uint16_t>(next_sequence_number_ + 1);

    bool const is_partial =
        (header.flags & rdma::communication::rdma_frame_flag_partial_payload) != 0;
    if (is_partial) {
        return;
    }

    std::size_t total_size = 0;
    for (auto const& frame : pending_rdma_frames_) {
        total_size += frame.payload.size();
    }

    std::vector<std::uint8_t> aggregated;
    aggregated.reserve(total_size);
    for (auto const& frame : pending_rdma_frames_) {
        aggregated.insert(aggregated.end(), frame.payload.begin(), frame.payload.end());
    }
    pending_rdma_frames_.clear();

    // TODO: avoid extra copy by feeding payload directly without socket_io.
    std::string payload_string(aggregated.begin(), aggregated.end());
    socket_io io(payload_string);
    auto message = replication_message::receive(io);
    if (! message) {
        LOG_LP(ERROR) << "RDMA failed to deserialize replication_message.";
        return;
    }
    if (message->get_message_type_id() != message_type_id::LOG_ENTRY) {
        LOG_LP(ERROR) << "RDMA unexpected message type: "
                      << static_cast<int>(message->get_message_type_id());
        return;
    }

    auto* log_entries = dynamic_cast<message_log_entries*>(message.get());
    if (! log_entries) {
        LOG_LP(ERROR) << "RDMA LOG_ENTRY cast failed.";
        return;
    }
    log_entries->set_session_end_flag(false);
    log_entries->set_flush_flag(false);

    // Apply entries but skip TCP ACK because this is the RDMA path.
    auto resources = create_handler_resources();
    log_entries->post_receive(*resources);

    auto& ack_io = resources->get_socket_io();
    int ack_fd = ack_io.get_socket_fd();
    if (ack_fd < 0) {
        LOG_LP(ERROR) << "RDMA ACK socket fd is invalid.";
        return;
    }

    rdma::communication::ack_message ack{};
    ack.version = rdma::communication::ack_protocol_version;
    ack.flags = 0U;
    ack.sequence_number = header.sequence_number;
    auto ack_result = rdma::communication::write_ack_message_to_fd(ack, ack_fd);
    if (! ack_result.success) {
        LOG_LP(ERROR) << "Failed to write RDMA ACK: " << ack_result.error_message;
    }
}

void log_channel_handler::dispatch(replication_message& message, handler_resources& resources) {
    message.post_receive(resources);
}

validation_result log_channel_handler::authorize() {
    int id = log_channel_id_counter.fetch_add(1, std::memory_order_seq_cst);
    if (id >= MAX_LOG_CHANNEL_COUNT) {
        LOG(ERROR) << "Exceeded maximum number of log channels: " << MAX_LOG_CHANNEL_COUNT;
        return validation_result::error(
            message_error::log_channel_error_too_many_channels,
            "Too many log channels: cannot assign more");
    }

    std::ostringstream oss;
    oss << "logch" << std::setw(5) << std::setfill('0') << id;
    pthread_setname_np(pthread_self(), oss.str().c_str());

    return validation_result::success();
}

void log_channel_handler::set_log_channel_id_counter_for_test(int value) {
    log_channel_id_counter.store(value, std::memory_order_seq_cst);
}

log_channel& log_channel_handler::get_log_channel() {
    return *log_channel_;
}

std::unique_ptr<handler_resources> log_channel_handler::create_handler_resources() {
    return std::make_unique<log_channel_handler_resources>(get_socket_io(), *log_channel_);
}

} // namespace limestone::replication
