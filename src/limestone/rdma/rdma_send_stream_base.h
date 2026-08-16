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
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <rdma/rdma_frame_buffer_base.h>

namespace limestone::replication {

/**
 * @brief Abstract interface for an RDMA send stream.
 *
 * Provides the minimal API used by limestone for sending data via RDMA,
 * independent of the rdma_comm library.  Null and rdma_comm-backed
 * implementations live in the same rdma/ directory.
 *
 * Sending follows an acquire/submit model: acquire_frame_buffer() lends the caller
 * a slice of the send ring, the caller writes the payload straight into it, and
 * submit_frame_buffer() writes the frame header and issues the RDMA write.  A frame
 * occupies its ring slots until the receiver acknowledges it, so a stalled peer
 * eventually blocks acquire_frame_buffer(); this is the flow control mechanism.
 *
 * @note Implementations are not thread-safe.  An acquire/submit pair, flush(), and
 *       any other call on the same stream must be issued from a single thread at a
 *       time; callers are responsible for serializing access.
 */
class rdma_send_stream_base {
public:
    /**
     * @brief Result of a submit_frame_buffer call.
     */
    struct send_result {
        bool        success{};          ///< true if the operation completed without error.
        std::string error_message;      ///< Diagnostic text when success is false.
        std::size_t bytes_written{};    ///< Number of bytes actually transferred.
    };

    /**
     * @brief Result of a flush call.
     */
    struct flush_result {
        bool        success{};       ///< true if all pending acks were received.
        std::string error_message;   ///< Diagnostic text when success is false.
    };

    rdma_send_stream_base() = default;

    rdma_send_stream_base(rdma_send_stream_base const&) = delete;
    rdma_send_stream_base& operator=(rdma_send_stream_base const&) = delete;
    rdma_send_stream_base(rdma_send_stream_base&&) = delete;
    rdma_send_stream_base& operator=(rdma_send_stream_base&&) = delete;

    virtual ~rdma_send_stream_base() = default;

    /**
     * @brief Acquire a writable frame buffer of at least @p min_capacity bytes.
     *
     * Blocks until the send ring can grant the slots, then returns a handle whose
     * payload region the caller fills directly.
     *
     * @param max_payload  Number of payload bytes the caller intends to write.
     * @param min_capacity Smallest capacity the caller can make progress with.  Must
     *                     be at least 1, at most @p max_payload, and no larger than
     *                     one send-ring slot's payload.
     * @return Acquired frame buffer, or nullptr when the request is out of bounds or
     *         the ring could not grant the slots.
     *
     * @warning Only capacity() >= @p min_capacity is guaranteed; the granted capacity
     *          may be smaller or larger than @p max_payload.  Callers must write
     *          min(capacity(), bytes they actually have) bytes, never capacity() bytes.
     */
    [[nodiscard]] virtual std::unique_ptr<rdma_frame_buffer_base> acquire_frame_buffer(
        std::size_t max_payload,
        std::size_t min_capacity) noexcept = 0;

    /**
     * @brief Write the frame header and submit a previously acquired frame buffer.
     * @param frame        Frame acquired from acquire_frame_buffer() on this stream.
     * @param payload_size Number of payload bytes written into the frame; must not
     *                     exceed the frame's capacity.
     * @return send_result describing status and the number of bytes submitted.
     *
     * @note The frame is consumed on success and must not be submitted again.
     * @note Not declared noexcept: the underlying rdma_comm submit path is not
     *       noexcept either, and building a diagnostic message may allocate.
     */
    [[nodiscard]] virtual send_result submit_frame_buffer(
        rdma_frame_buffer_base& frame,
        std::size_t             payload_size) = 0;

    /**
     * @brief Wait until all outstanding acknowledgements are received.
     * @param timeout Maximum duration to wait.
     * @return flush_result describing success or failure.
     */
    [[nodiscard]] virtual flush_result flush(std::chrono::milliseconds timeout) noexcept = 0;

    /**
     * @brief Send an entire byte range, splitting it across as many frames as needed.
     *
     * Acquires a frame, copies into it, and submits, until the whole range is out.  This
     * is where the clamp that the acquire_frame_buffer() warning demands lives: each chunk is
     * min(granted capacity, bytes left), so an over-generous grant cannot make the copy
     * read past the end of @p payload.  Callers should use this rather than open-coding
     * the loop.
     *
     * @param payload Bytes to send.  Sending an empty range is a no-op and succeeds.
     * @return send_result with bytes_written set to the number of bytes submitted.  On
     *         failure the range is partially sent and bytes_written says how far it got.
     *
     * @note Non-virtual: implementations customize acquire/submit, not the loop over them.
     * @note Not declared noexcept, for the same reason submit_frame_buffer() is not.
     */
    [[nodiscard]] send_result send_all_bytes(std::string_view payload);
};

} // namespace limestone::replication
