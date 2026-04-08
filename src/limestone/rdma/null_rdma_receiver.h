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

#include <rdma/rdma_receiver_base.h>

namespace limestone::replication {

/**
 * @brief Null implementation of rdma_receiver_base.
 *
 * initialize() returns a failure result and get_dma_address() returns std::nullopt
 * because RDMA is not available in this build configuration.
 * register_channel() and shutdown() succeed without performing any RDMA operations.
 */
class null_rdma_receiver : public rdma_receiver_base {
public:
    null_rdma_receiver() = default;
    ~null_rdma_receiver() override = default;

    null_rdma_receiver(null_rdma_receiver const&) = delete;
    null_rdma_receiver& operator=(null_rdma_receiver const&) = delete;
    null_rdma_receiver(null_rdma_receiver&&) = delete;
    null_rdma_receiver& operator=(null_rdma_receiver&&) = delete;

    [[nodiscard]] operation_result initialize(rdma_receive_handler handler) noexcept override;
    [[nodiscard]] operation_result shutdown() noexcept override;
    [[nodiscard]] operation_result register_channel(std::uint16_t channel_id, int ack_socket) noexcept override;
    [[nodiscard]] std::optional<std::uint64_t> get_dma_address() const noexcept override;
};

} // namespace limestone::replication
