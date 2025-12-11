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

#include <replication/replication_message.h>

namespace limestone::replication {

/**
 * @brief RDMA init ACK that carries remote DMA address.
 */
class message_rdma_init_ack : public replication_message {
public:
    /**
     * @brief Constructs a message_rdma_init_ack with the specified remote DMA address.

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(socket_io& io) const override;


    /**

    [[nodiscard]] uint64_t get_remote_dma_address() const { return remote_dma_address_; }
    /**
     * @brief Sets the remote DMA address.
     * @param remote_dma_address The remote DMA address to set.
     */
     * @brief Returns the remote DMA address carried by this message.
     * @return The remote DMA address as a 64-bit unsigned integer.
     */
     * @return std::unique_ptr<replication_message> The created message instance.
     */
     * @brief Receives the body of the RDMA init ACK message from the given socket.
     * @param io The socket I/O object to receive data from.
     */
    /**
     * @brief Sends the body of the RDMA init ACK message via the given socket.
     * @param io The socket_io object used to send the message body.
     */
     * @brief Returns the message type identifier for this message.
     * @return The message type identifier.
     */
     */
    explicit message_rdma_init_ack(uint64_t remote_dma_address = 0) noexcept
        : remote_dma_address_(remote_dma_address) {}

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(socket_io& io) const override;
    void receive_body(socket_io& io) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

    [[nodiscard]] uint64_t get_remote_dma_address() const { return remote_dma_address_; }
    void set_remote_dma_address(uint64_t remote_dma_address) {
        remote_dma_address_ = remote_dma_address;
    }

private:
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::RDMA_INIT_ACK, &message_rdma_init_ack::create);
        return true;
    }();

    uint64_t remote_dma_address_{};
};

}  // namespace limestone::replication

