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

/// @brief Callback type invoked for each RDMA receive event.
using rdma_receive_handler = std::function<void(rdma_receive_event const&)>;

/**
 * @brief Abstract interface for an RDMA receiver.
 *
 * Wraps lifecycle management (initialize / shutdown / register_channel /
 * get_dma_address) independently of the rdma_comm library.
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
     * @brief Register an acknowledgement endpoint for the given logical channel.
     * @param channel_id  Logical channel identifier.
     * @param ack_socket  Borrowed socket file descriptor used to send acknowledgements.
     *                    Implementations must duplicate it before taking ownership.
     * @return operation_result describing success or failure.
     */
    [[nodiscard]] virtual operation_result register_channel(
        std::uint16_t channel_id,
        int           ack_socket) noexcept = 0;

    /**
     * @brief Retrieve the DMA address allocated for the shared receive buffer.
     * @return DMA address when initialized; std::nullopt otherwise.
     */
    [[nodiscard]] virtual std::optional<std::uint64_t> get_dma_address() const noexcept = 0;
};

} // namespace limestone::replication
