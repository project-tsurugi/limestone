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

#include "rdma_log_channel_receiver.h"

#include <string_view>

#include <glog/logging.h>

#include "logging_helper.h"
#include "message_log_entries.h"

namespace limestone::replication {

rdma_log_channel_receiver::rdma_log_channel_receiver(limestone::api::datastore& datastore,
    limestone::api::log_channel& channel) noexcept
    : datastore_(datastore)
    , channel_(channel) {}

void rdma_log_channel_receiver::handle_rdma_data_event(rdma_data_event const& event) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto const& header = event.header;
        TRACE_START << "seq=" << header.sequence_number
                    << " size=" << header.payload_size
                    << " partial=" << ((header.flags
                                        & rdma_frame_flag_partial_payload) != 0)
                    << " pending=" << pending_frames_.size()
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
        pending_frames_.push_back(event);
        next_sequence_number_ = static_cast<std::uint16_t>(next_sequence_number_ + 1);
        process_pending_frames_locked();
    }
}

void rdma_log_channel_receiver::process_pending_frames_locked() {
    while (true) {
        if (pending_frames_.empty()) {
            return;
        }

        auto event = std::move(pending_frames_.front());
        pending_frames_.erase(pending_frames_.begin());
        process_payload_locked(event.payload, event.header);
    }
}

void rdma_log_channel_receiver::process_payload_locked(
    std::vector<std::uint8_t> const& payload,
    rdma_frame_header const& last_header) {
    TRACE_START << "seq=" << last_header.sequence_number
                << " payload_size=" << payload.size();
    if (!entries_receiver_) {
        entries_receiver_ = std::make_unique<rdma_log_entries_receiver>(datastore_);
    }

    std::string_view bytes{
        reinterpret_cast<char const*>(payload.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        payload.size()};
    try {
        // entries_receiver_ either consumes the full payload or throws on
        // protocol errors, so there is no partial-consume remainder to handle.
        static_cast<void>(entries_receiver_->consume(bytes));

        while (entries_receiver_->has_message()) {
            auto log_entries = entries_receiver_->take_message();
            log_entries->apply_to(channel_);
        }
    } catch (std::exception const& e) {
        LOG_LP(FATAL) << "RDMA receiver failed while processing payload: "
                      << e.what();
    }

    TRACE_END;
}

void rdma_log_channel_receiver::push_pending_frame_for_test(rdma_data_event const& event) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_frames_.push_back(event);
    }
}

limestone::api::log_channel& rdma_log_channel_receiver::get_log_channel() noexcept {
    return channel_;
}

} // namespace limestone::replication
