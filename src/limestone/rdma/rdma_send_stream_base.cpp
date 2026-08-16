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
#include <rdma/rdma_send_stream_base.h>

#include <algorithm>
#include <cstddef>

namespace limestone::replication {

rdma_send_stream_base::send_result rdma_send_stream_base::send_all_bytes(
        std::string_view payload) {
    std::size_t offset = 0;
    while (offset < payload.size()) {
        auto const remaining = payload.size() - offset;
        auto frame = acquire_frame_buffer(remaining, 1);
        if (! frame) {
            return {false, "failed to acquire an RDMA frame buffer", offset};
        }
        // The grant is only guaranteed to be at least min_capacity: it can fall short of
        // what was asked for (single-DMA cap, ring wrap) and it can also overshoot it
        // (the ring hands out whole slots). Clamp to the bytes that are actually left, or
        // the copy below would read past the end of payload.
        auto const chunk = std::min(frame->capacity(), remaining);
        std::copy_n(payload.data() + offset, chunk, frame->payload());
        auto result = submit_frame_buffer(*frame, chunk);
        if (! result.success) {
            return {false, result.error_message, offset};
        }
        offset += chunk;
    }
    return {true, "", offset};
}

} // namespace limestone::replication
