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
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <rdma/rdma_send_stream_base.h>

namespace limestone::replication {

/**
 * @brief Abstract interface for an RDMA sender.
 *
 * Wraps the lifecycle management (initialize / get_send_stream / shutdown)
 * independently of the rdma_comm library.
 */
class rdma_sender_base {
public:
    /**
     * @brief Result of initialize / shutdown operations.
     */
    struct operation_result {
        bool        success{};       ///< true if the operation succeeded.
        std::string error_message;   ///< Diagnostic text when success is false.
    };

    /**
     * @brief Result returned by get_send_stream.
     */
    struct stream_acquire_result {
        operation_result                         status;   ///< Outcome of the acquisition.
        std::unique_ptr<rdma_send_stream_base>   stream;  ///< Acquired stream (null on failure).
    };

    rdma_sender_base() = default;

    rdma_sender_base(rdma_sender_base const&) = delete;
    rdma_sender_base& operator=(rdma_sender_base const&) = delete;
    rdma_sender_base(rdma_sender_base&&) = delete;
    rdma_sender_base& operator=(rdma_sender_base&&) = delete;

    virtual ~rdma_sender_base() = default;

    /**
     * @brief Initialize the sender and establish the control plane.
     * @param remote_dma_address DMA address exposed by the receiver peer.
     * @return operation_result describing success or failure.
     */
    [[nodiscard]] virtual operation_result initialize(std::uint64_t remote_dma_address) noexcept = 0;

    /**
     * @brief Acquire a send stream for the given logical channel.
     * @param channel_id Logical channel identifier.
     * @param ack_fd     Borrowed file descriptor used to receive acknowledgements.
     *                   Implementations must duplicate it before taking ownership.
     * @return stream_acquire_result; stream is non-null on success.
     */
    [[nodiscard]] virtual stream_acquire_result get_send_stream(
        std::uint16_t channel_id,
        int           ack_fd) noexcept = 0;

    /**
     * @brief Shut down the sender and release all resources.
     * @return operation_result describing success or failure.
     */
    [[nodiscard]] virtual operation_result shutdown() noexcept = 0;
};

} // namespace limestone::replication
