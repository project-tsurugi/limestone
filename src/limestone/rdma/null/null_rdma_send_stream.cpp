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
#include <rdma/null_rdma_send_stream.h>

#include <new>

namespace limestone::replication {

std::unique_ptr<rdma_frame_buffer_base> null_rdma_send_stream::acquire_frame_buffer(
        std::size_t max_payload,
        std::size_t min_capacity) noexcept {
    if (min_capacity == 0 || min_capacity > max_payload) {
        return nullptr;
    }
    // Grant the full request: the null stream has no ring to wrap around.
    try {
        return std::make_unique<null_rdma_frame_buffer>(max_payload);
    } catch (std::bad_alloc const&) {
        return nullptr;
    }
}

rdma_send_stream_base::send_result null_rdma_send_stream::submit_frame_buffer(
        rdma_frame_buffer_base& frame,
        std::size_t             payload_size) {
    if (payload_size > frame.capacity()) {
        return {false,
            "null_rdma_send_stream::submit_frame_buffer: payload_size exceeds capacity", 0};
    }
    return {true, "", payload_size};
}

rdma_send_stream_base::flush_result null_rdma_send_stream::flush(
        std::chrono::milliseconds /*timeout*/) noexcept {
    return {true, ""};
}

} // namespace limestone::replication
