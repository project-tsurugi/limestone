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

#include <string>
#include <vector>

#include <glog/logging.h>
#include <rdma_comm/rdma_receiver.h>
#include <rdma_comm/rdma_frame_header.h>
#include <rdma_comm/ack_message.h>

#include "replication_message.h"
#include "message_error.h"
#include "message_log_channel_create.h"
#include "message_log_entries.h"
#include "validation_result.h"
#include "socket_io.h"
#include "logging_helper.h"

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
    std::lock_guard<std::mutex> lock(rdma_mutex_);
    auto const& header = event.header;
    TRACE_START << "seq=" << header.sequence_number
                << " size=" << header.payload_size
                << " partial=" << ((header.flags
                                    & rdma::communication::rdma_frame_flag_partial_payload) != 0)
                << " pending=" << pending_rdma_frames_.size()
                << " future=" << future_rdma_frames_.size()
                << " next_expected=" << next_sequence_number_;
    if (header.version != rdma::communication::rdma_frame_protocol_version) {
        LOG_LP(ERROR) << "RDMA frame version mismatch: expected "
                      << static_cast<int>(rdma::communication::rdma_frame_protocol_version)
                      << " got " << static_cast<int>(header.version);
        pending_rdma_frames_.clear();
        future_rdma_frames_.clear();
        TRACE_ABORT << "version mismatch";
        return;
    }

    if (header.payload_size != event.payload.size()) {
        LOG_LP(ERROR) << "RDMA payload size mismatch: header=" << header.payload_size
                      << " actual=" << event.payload.size();
        pending_rdma_frames_.clear();
        future_rdma_frames_.clear();
        TRACE_ABORT << "payload size mismatch";
        return;
    }

    if (header.sequence_number < next_sequence_number_) {
        LOG_LP(INFO) << "RDMA duplicate or stale frame: expected="
                     << next_sequence_number_ << " received=" << header.sequence_number;
        TRACE_ABORT << "stale frame";
        return;
    }

    if (header.sequence_number > next_sequence_number_) {
        LOG_LP(INFO) << "RDMA sequence gap: expected=" << next_sequence_number_
                     << " received=" << header.sequence_number;
        future_rdma_frames_[header.sequence_number] = event;
        TRACE_ABORT << "sequence gap, stored for future";
        return;
    }
    pending_rdma_frames_.push_back(event);
    next_sequence_number_ = static_cast<std::uint16_t>(next_sequence_number_ + 1);

    auto it = future_rdma_frames_.find(next_sequence_number_);
    while (it != future_rdma_frames_.end()) {
        pending_rdma_frames_.push_back(it->second);
        future_rdma_frames_.erase(it);
        next_sequence_number_ =
            static_cast<std::uint16_t>(next_sequence_number_ + 1);
        it = future_rdma_frames_.find(next_sequence_number_);
    }
    process_pending_rdma_messages_locked();
}

void log_channel_handler::process_pending_rdma_messages_locked() {
    while (true) {
        if (pending_rdma_frames_.empty()) {
            return;
        }

        std::size_t message_end_index = pending_rdma_frames_.size();
        std::size_t total_size = 0;
        for (std::size_t idx = 0; idx < pending_rdma_frames_.size(); ++idx) {
            auto const& frame = pending_rdma_frames_[idx];
            total_size += frame.payload.size();
            bool const is_partial =
                (frame.header.flags &
                 rdma::communication::rdma_frame_flag_partial_payload) != 0;
            if (! is_partial) {
                message_end_index = idx;
                break;
            }
        }
        if (message_end_index == pending_rdma_frames_.size()) {
            // No complete message yet (all frames are partial so far).
            return;
        }

        std::vector<std::uint8_t> aggregated;
        aggregated.reserve(total_size);
        for (std::size_t idx = 0; idx <= message_end_index; ++idx) {
            auto const& frame = pending_rdma_frames_[idx];
            aggregated.insert(aggregated.end(), frame.payload.begin(), frame.payload.end());
        }
        auto last_header = pending_rdma_frames_[message_end_index].header;
        pending_rdma_frames_.erase(
            pending_rdma_frames_.begin(),
            pending_rdma_frames_.begin()
                + static_cast<std::ptrdiff_t>(message_end_index + 1));

        process_rdma_message_locked(aggregated, last_header);
    }
}

void log_channel_handler::process_rdma_message_locked(
    std::vector<std::uint8_t> const& payload,
    rdma::communication::rdma_frame_header const& last_header) {
    TRACE_START << "frames_for_ack_seq=" << last_header.sequence_number
                << " payload_size=" << payload.size();
    // TODO: avoid extra copy by feeding payload directly without socket_io.
    std::string payload_string(payload.begin(), payload.end());
    socket_io io(payload_string);
    auto message = replication_message::receive(io);
    if (! message) {
        LOG_LP(ERROR) << "RDMA failed to deserialize replication_message.";
        TRACE_ABORT << "deserialize failed";
        return;
    }
    if (message->get_message_type_id() != message_type_id::LOG_ENTRY) {
        LOG_LP(ERROR) << "RDMA unexpected message type: "
                      << static_cast<int>(message->get_message_type_id());
        TRACE_ABORT << "unexpected message type id=" << static_cast<int>(message->get_message_type_id());
        return;
    }

    auto* log_entries = dynamic_cast<message_log_entries*>(message.get());
    if (! log_entries) {
        LOG_LP(ERROR) << "RDMA LOG_ENTRY cast failed.";
        TRACE_ABORT << "cast failed";
        return;
    }

    // Apply entries but skip TCP ACK because this is the RDMA path.
    auto resources = std::make_unique<log_channel_handler_resources>(get_socket_io(), *log_channel_, true);
    log_entries->post_receive(*resources);

    TRACE_END;
}

void log_channel_handler::push_pending_frame_for_test(
    rdma::communication::rdma_receive_data_event const& event) {
    std::lock_guard<std::mutex> lock(rdma_mutex_);
    pending_rdma_frames_.push_back(event);
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
    return std::make_unique<log_channel_handler_resources>(get_socket_io(), *log_channel_, true);
}

} // namespace limestone::replication
