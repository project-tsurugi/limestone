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
#include <rdma/rdma_comm_send_stream.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <string>
#include <thread>

namespace limestone::replication {

namespace {

// Bound on consecutive undersized-frame retries at a ring-wrap boundary. A healthy
// ring resolves a wrap in one retry; a higher cap tolerates transient contention
// while still failing fast instead of spinning forever on a stuck ring.
constexpr unsigned int max_undersized_retries = 16U;

constexpr char const* acquire_failed_error = "RDMA acquire_frame_buffer failed";

// submit_frame_buffer is not noexcept, but every public method here is. Funnel all
// submissions through this guard so an unexpected exception becomes a failed result
// instead of terminating the process.
rdma_send_stream_base::send_result submit_guarded(
        rdma::communication::rdma_send_stream& stream,
        rdma::communication::rdma_send_stream::frame_buffer& frame,
        std::size_t payload_size) noexcept {
    try {
        auto r = stream.submit_frame_buffer(frame, payload_size);
        return {r.success, r.error_message, r.bytes_written};
    } catch (std::exception const& e) {
        return {false, std::string{"RDMA submit_frame_buffer threw: "} + e.what(), 0U};
    } catch (...) {
        return {false, "RDMA submit_frame_buffer threw an unknown exception", 0U};
    }
}

}  // namespace

rdma_comm_send_stream::rdma_comm_send_stream(
        std::unique_ptr<rdma::communication::rdma_send_stream> stream)
    : stream_(std::move(stream))
{}

rdma::communication::rdma_send_stream::frame_buffer
rdma_comm_send_stream::acquire_frame_min_capacity(
        std::size_t request_size,
        std::size_t min_capacity) noexcept {
    unsigned int undersized_retries = 0U;
    while (true) {
        auto frame = stream_->acquire_frame_buffer(request_size);
        if (! frame.valid()) {
            return frame;  // pool unavailable or exhausted
        }
        // Defensive path, effectively unreachable in production: a single slot's capacity
        // already far exceeds the few bytes of min_capacity limestone ever requests, so any
        // valid frame satisfies it. It is kept to stay correct under tiny slot/DMA
        // configurations or a future caller demanding a larger minimum, mirroring
        // blob_relay's acquire_frame_min_capacity, and is exercised only by a dedicated
        // test fake (never by real traffic).
        if (frame.capacity < min_capacity) {
            // The acquired span is too small to co-locate the caller's header and its
            // first chunk (ring-wrap boundary). Release it (RAII when the loop-local
            // frame is destroyed on the next iteration) and retry from the ring start.
            if (++undersized_retries > max_undersized_retries) {
                return rdma::communication::rdma_send_stream::frame_buffer{};
            }
            std::this_thread::yield();
            continue;
        }
        return frame;
    }
}

rdma_send_stream_base::send_result rdma_comm_send_stream::send_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept {
    if (offset > payload.size()) {
        return {false, "send_bytes offset exceeds payload size", 0U};
    }
    // length is a maximum: clamp to the bytes available from offset, matching the
    // documented send_bytes contract. An offset past the end is rejected above.
    auto const to_send = std::min(length, payload.size() - offset);
    if (to_send == 0U) {
        return {true, "", 0U};
    }
    auto frame = acquire_frame_min_capacity(to_send, 0U);
    if (! frame.valid()) {
        return {false, acquire_failed_error, 0U};
    }
    auto const chunk = std::min(to_send, frame.capacity);
    std::memcpy(frame.payload, payload.data() + offset, chunk);
    return submit_guarded(*stream_, frame, chunk);
}

rdma_send_stream_base::send_result rdma_comm_send_stream::send_all_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept {
    if (offset > payload.size()) {
        return {false, "send_all_bytes offset exceeds payload size", 0U};
    }
    // length is a maximum: clamp to the bytes available from offset, matching the
    // documented send_all_bytes contract. An offset past the end is rejected above.
    auto const to_send = std::min(length, payload.size() - offset);
    std::size_t processed = 0U;
    while (processed < to_send) {
        auto const remaining = to_send - processed;
        auto frame = acquire_frame_min_capacity(remaining, 0U);
        if (! frame.valid()) {
            return {false, acquire_failed_error, processed};
        }
        auto const chunk = std::min(remaining, frame.capacity);
        std::memcpy(frame.payload, payload.data() + offset + processed, chunk);
        auto const r = submit_guarded(*stream_, frame, chunk);
        if (! r.success) {
            return {false, r.error_message, processed};
        }
        if (r.bytes_written == 0U) {
            return {false, "RDMA submit_frame_buffer returned zero bytes", processed};
        }
        processed += r.bytes_written;
    }
    return {true, "", processed};
}

rdma_send_stream_base::flush_result rdma_comm_send_stream::flush(
        std::chrono::milliseconds timeout) noexcept {
    auto r = stream_->flush(timeout);
    return {r.success, r.error_message};
}

rdma_send_stream_base::send_result rdma_comm_send_stream::send_with_writer(
        std::size_t   remaining_size,
        buffer_writer writer,
        std::size_t   min_capacity) noexcept {
    if (remaining_size == 0U) {
        return {true, "", 0U};
    }
    // A minimum larger than the whole payload is meaningless; clamp it so the frame
    // contract (capacity in [effective_min, remaining_size]) stays satisfiable. remaining_size
    // is non-zero here, so the floor of 1 keeps effective_min consistent with the documented
    // `max(1, min(min_capacity, remaining_size))` guarantee.
    auto const effective_min = std::max<std::size_t>(1U, std::min(min_capacity, remaining_size));
    auto frame = acquire_frame_min_capacity(remaining_size, effective_min);
    if (! frame.valid()) {
        return {false, acquire_failed_error, 0U};
    }
    auto const capacity = std::min(remaining_size, frame.capacity);
    auto const fill_result = writer(frame.payload, capacity);
    if (! fill_result.success) {
        // frame is released without submission via RAII on scope exit.
        return {false, fill_result.error_message, 0U};
    }
    return submit_guarded(*stream_, frame, capacity);
}

} // namespace limestone::replication
