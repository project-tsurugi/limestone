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
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "replication_message.h"
#include "message_error.h"
#include "message_log_channel_create.h"
#include "message_log_entries.h"
#include "validation_result.h"
#include "replication_message_io.h"
#include "logging_helper.h"

namespace limestone::replication {

log_channel_handler::log_channel_handler(replica_server &server, replication_message_io& io) noexcept
    : channel_handler_base(server, io){}

log_channel_handler::log_channel_handler(replica_server& server, rdma_only_tag tag)
    : log_channel_handler(server, tag,
                          std::make_unique<replication_message_io>(std::string{})) {}

log_channel_handler::log_channel_handler(replica_server& server, rdma_only_tag /*tag*/,
                                         std::unique_ptr<replication_message_io> sentinel_io) noexcept
    : channel_handler_base(server, *sentinel_io)
    , sentinel_io_(std::move(sentinel_io)) {}

void log_channel_handler::bind_log_channel(log_channel& channel) noexcept {
    log_channel_ = &channel;
}

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

    // TODO Add other validation processes

    auto& ds = get_server().get_datastore();    
    log_channel_ = &ds.create_channel();
    

    // Perform additional validation as needed
    return validation_result::success();
}

void log_channel_handler::send_initial_ack() const {
    send_ack();
}

void log_channel_handler::handle_rdma_data_event(
    rdma_data_event const& event) {
    std::lock_guard<std::mutex> lock(rdma_mutex_);
    auto const& header = event.header;
    TRACE_START << "seq=" << header.sequence_number
                << " size=" << header.payload_size
                << " partial=" << ((header.flags
                                    & rdma_frame_flag_partial_payload) != 0)
                << " pending=" << pending_rdma_frames_.size()
                << " next_expected=" << next_sequence_number_;
    if (header.version != rdma_frame_current_version) {
        LOG_LP(FATAL) << "RDMA frame version mismatch: expected "
                      << static_cast<int>(rdma_frame_current_version)
                      << " got " << static_cast<int>(header.version);
    }

    if (header.payload_size != event.payload.size()) {
        LOG_LP(FATAL) << "RDMA payload size mismatch: header=" << header.payload_size
                      << " actual=" << event.payload.size();
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
        TRACE_ABORT << "sequence gap, dropped";
        return;
    }
    pending_rdma_frames_.push_back(event);
    next_sequence_number_ = static_cast<std::uint16_t>(next_sequence_number_ + 1);
    process_pending_rdma_messages_locked();
}

void log_channel_handler::process_pending_rdma_messages_locked() {
    while (true) {
        if (pending_rdma_frames_.empty()) {
            return;
        }

        auto event = std::move(pending_rdma_frames_.front());
        pending_rdma_frames_.erase(pending_rdma_frames_.begin());
        process_rdma_message_locked(event.payload, event.header);
    }
}

void log_channel_handler::process_rdma_message_locked(
    std::vector<std::uint8_t> const& payload,
    rdma_frame_header const& last_header) {
    TRACE_START << "frames_for_ack_seq=" << last_header.sequence_number
                << " payload_size=" << payload.size();
    if (!rdma_receiver_) {
        rdma_receiver_ = std::make_unique<rdma_log_entries_receiver>(get_server().get_datastore());
    }

    std::string_view bytes{
        reinterpret_cast<char const*>(payload.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        payload.size()};
    try {
        // The receiver consumes the full payload or throws on protocol errors,
        // so there is no partial-consume path to handle here.
        static_cast<void>(rdma_receiver_->consume(bytes));

        while (rdma_receiver_->has_message()) {
            auto log_entries = rdma_receiver_->take_message();
            auto resources = std::make_unique<log_channel_handler_resources>(get_replication_message_io(), *log_channel_, false);
            log_entries->post_receive(*resources);
        }
    } catch (std::exception const& e) {
        LOG_LP(FATAL) << "RDMA receiver failed while processing payload: "
                      << e.what();
    }

    TRACE_END;
}

void log_channel_handler::push_pending_frame_for_test(
    rdma_data_event const& event) {
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
    return std::make_unique<log_channel_handler_resources>(get_replication_message_io(), *log_channel_, true);
}

} // namespace limestone::replication
