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
#include <functional>
#include <optional>
#include <string>

#include <rdma/rdma_receive_event.h>

namespace limestone::replication {

class rdma_sender_base;

/// @brief Callback type invoked for each RDMA receive event.
using rdma_receive_handler = std::function<void(rdma_receive_event const&)>;

/**
 * @brief Abstract interface for an RDMA receiver.
 *
 * Wraps lifecycle management (initialize / shutdown / get_dma_address)
 * independently of the rdma_comm library.
 */
class rdma_receiver_base {
public:
    /**
     * @brief Result of receiver operations.
     */
    struct operation_result {
        bool        success{};       ///< true if the operation succeeded.
        std::string error_message;   ///< Diagnostic text when success is false.
    };

    rdma_receiver_base() = default;

    rdma_receiver_base(rdma_receiver_base const&) = delete;
    rdma_receiver_base& operator=(rdma_receiver_base const&) = delete;
    rdma_receiver_base(rdma_receiver_base&&) = delete;
    rdma_receiver_base& operator=(rdma_receiver_base&&) = delete;

    virtual ~rdma_receiver_base() = default;

    /**
     * @brief Initialize the receiver and start listening for incoming frames.
     * @param handler Callback invoked for each completed receive event.
     * @return operation_result describing success or failure.
     */
    [[nodiscard]] virtual operation_result initialize(rdma_receive_handler handler) noexcept = 0;

    /**
     * @brief Shut down the receiver and release all resources.
     * @return operation_result describing success or failure.
     */
    [[nodiscard]] virtual operation_result shutdown() noexcept = 0;

    /**
     * @brief Retrieve the DMA address allocated for the shared receive buffer.
     * @return DMA address when initialized; std::nullopt otherwise.
     */
    [[nodiscard]] virtual std::optional<std::uint64_t> get_dma_address() const noexcept = 0;

    /**
     * @brief Bind the sender used for the RDMA ACK return path and transition
     *        from SETUP to TRANSFER phase.
     * @param sender RDMA sender instance whose buffer is used as the ACK destination.
     * @param local_instance_id Our own side's instance_id; attached to the ACK frames this
     *        receiver sends.
     * @param remote_instance_id The peer side's instance_id (the replica's on the master, the
     *        master's on the replica). Received frames are checked against it, and those that
     *        do not match are dropped.
     * @return operation_result describing success or failure.
     * @note Must be called before the receiver starts delivering RDMA frames so
     *       that ACK frames can be RDMA-written back through @p sender.
     * @note Zero is rejected for either instance_id.
     */
    [[nodiscard]] virtual operation_result finalize_channel_setup_with_sender(
        rdma_sender_base* sender,
        std::uint64_t     local_instance_id,
        std::uint64_t     remote_instance_id) noexcept = 0;
};

} // namespace limestone::replication
