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
     * @brief Construct ACK with remote DMA address.
     * @param remote_dma_address remote DMA address to include in the ACK.
     */
    explicit message_rdma_init_ack(uint64_t remote_dma_address = 0) noexcept
        : remote_dma_address_(remote_dma_address) {}

    /**
     * @brief Get the message type identifier.
     * @return RDMA_INIT_ACK type id.
     */
    [[nodiscard]] message_type_id get_message_type_id() const override;

    /**
     * @brief Serialize body.
     * @param io socket I/O.
     */
    void send_body(socket_io& io) const override;

    /**
     * @brief Deserialize body.
     * @param io socket I/O.
     */
    void receive_body(socket_io& io) override;

    /**
     * @brief Factory function.
     * @return message instance.
     */
    [[nodiscard]] static std::unique_ptr<replication_message> create();

    /**
     * @brief Get remote DMA address.
     * @return remote DMA address.
     */
    [[nodiscard]] uint64_t get_remote_dma_address() const { return remote_dma_address_; }

    /**
     * @brief Set remote DMA address.
     * @param remote_dma_address value to set.
     */
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
