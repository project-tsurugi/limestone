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
#include <rdma/rdma_comm_send_stream.h>

#include <logging_helper.h>
#include <rdma/rdma_comm/rdma_comm_constants.h>

namespace limestone::replication {

rdma_comm_send_stream::rdma_comm_send_stream(
        std::unique_ptr<rdma::communication::rdma_send_stream> stream)
    : stream_(std::move(stream))
{}

std::unique_ptr<rdma_frame_buffer_base> rdma_comm_send_stream::acquire_frame_buffer(
        std::size_t max_payload,
        std::size_t min_capacity) noexcept {
    if (min_capacity == 0 || min_capacity > max_payload || min_capacity > rdma_slot_payload_bytes) {
        LOG_LP(ERROR) << "invalid frame buffer request: min_capacity=" << min_capacity
                      << " max_payload=" << max_payload
                      << " rdma_slot_payload_bytes=" << rdma_slot_payload_bytes;
        return nullptr;
    }
    auto frame = stream_->acquire_frame_buffer(max_payload);
    if (! frame.valid()) {
        return nullptr;
    }
    if (frame.capacity < min_capacity) {
        // A valid frame spans at least one slot and min_capacity is capped to one
        // slot's payload, so this indicates a broken transport contract.
        LOG_LP(ERROR) << "acquired frame smaller than min_capacity: capacity=" << frame.capacity
                      << " min_capacity=" << min_capacity;
        return nullptr;
    }
    return std::make_unique<rdma_comm_frame_buffer>(std::move(frame));
}

rdma_send_stream_base::send_result rdma_comm_send_stream::submit_frame_buffer(
        rdma_frame_buffer_base& frame,
        std::size_t             payload_size) {
    // The rdma_*_base abstractions exist only as an ENABLE_RDMA build-time toggle:
    // null_* and rdma_comm_* implementations never coexist in one process, and a frame
    // is always submitted to the stream that acquired it. The cast is a cheap guard for
    // that invariant, not a dispatch over several frame kinds.
    auto* comm_frame = dynamic_cast<rdma_comm_frame_buffer*>(&frame);
    if (comm_frame == nullptr) {
        return {false, "rdma_comm_send_stream::submit_frame_buffer: frame is not an "
                       "rdma_comm_frame_buffer instance", 0};
    }
    auto r = stream_->submit_frame_buffer(comm_frame->native_frame(), payload_size);
    return {r.success, r.error_message, r.bytes_written};
}

rdma_send_stream_base::flush_result rdma_comm_send_stream::flush(
        std::chrono::milliseconds timeout) noexcept {
    auto r = stream_->flush(timeout);
    return {r.success, r.error_message};
}

} // namespace limestone::replication
