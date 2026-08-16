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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <rdma_comm/rdma_sender.h>

#include <rdma/rdma_frame_buffer_base.h>
#include <rdma/rdma_send_stream_base.h>

namespace limestone::replication {

/**
 * @brief rdma_frame_buffer_base implementation holding an rdma_comm frame_buffer.
 *
 * Owns the acquired frame for its whole lifetime: destroying this object without
 * submitting it releases the send-buffer slots back to the pool, which is the
 * release-on-drop contract rdma_frame_buffer_base promises.
 */
class rdma_comm_frame_buffer : public rdma_frame_buffer_base {
public:
    /**
     * @brief Construct from a frame acquired via rdma_send_stream::acquire_frame_buffer().
     * @param frame Acquired frame; ownership is transferred.
     */
    explicit rdma_comm_frame_buffer(
            rdma::communication::rdma_send_stream::frame_buffer frame) noexcept
        : frame_(std::move(frame))
    {}

    ~rdma_comm_frame_buffer() override = default;

    rdma_comm_frame_buffer(rdma_comm_frame_buffer const&) = delete;
    rdma_comm_frame_buffer& operator=(rdma_comm_frame_buffer const&) = delete;
    rdma_comm_frame_buffer(rdma_comm_frame_buffer&&) = delete;
    rdma_comm_frame_buffer& operator=(rdma_comm_frame_buffer&&) = delete;

    [[nodiscard]] std::uint8_t* payload() noexcept override { return frame_.payload; }

    [[nodiscard]] std::size_t capacity() const noexcept override { return frame_.capacity; }

    /**
     * @brief Returns the wrapped frame_buffer handed to the underlying API on submit.
     */
    [[nodiscard]] rdma::communication::rdma_send_stream::frame_buffer& native_frame() noexcept {
        return frame_;
    }

private:
    rdma::communication::rdma_send_stream::frame_buffer frame_;
};

/**
 * @brief rdma_send_stream_base implementation backed by rdma::communication::rdma_send_stream.
 *
 * Wraps an rdma_send_stream instance acquired via rdma::communication::rdma_sender::get_send_stream()
 * and delegates all calls to it, converting result types to the limestone-internal equivalents.
 */
class rdma_comm_send_stream : public rdma_send_stream_base {
public:
    /**
     * @brief Construct from an acquired rdma_comm send stream.
     * @param stream Ownership of the rdma_comm stream. Must be non-null.
     */
    explicit rdma_comm_send_stream(std::unique_ptr<rdma::communication::rdma_send_stream> stream);

    ~rdma_comm_send_stream() override = default;

    rdma_comm_send_stream(rdma_comm_send_stream const&) = delete;
    rdma_comm_send_stream& operator=(rdma_comm_send_stream const&) = delete;
    rdma_comm_send_stream(rdma_comm_send_stream&&) = delete;
    rdma_comm_send_stream& operator=(rdma_comm_send_stream&&) = delete;

    [[nodiscard]] std::unique_ptr<rdma_frame_buffer_base> acquire_frame_buffer(
        std::size_t max_payload,
        std::size_t min_capacity) noexcept override;

    [[nodiscard]] send_result submit_frame_buffer(
        rdma_frame_buffer_base& frame,
        std::size_t             payload_size) override;

    [[nodiscard]] flush_result flush(std::chrono::milliseconds timeout) noexcept override;

private:
    std::unique_ptr<rdma::communication::rdma_send_stream> stream_;
};

} // namespace limestone::replication
