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

#include <memory>

#include <rdma_comm/rdma_receiver.h>

#include <rdma/rdma_receiver_base.h>

namespace limestone::replication {

/**
 * @brief rdma_receiver_base implementation backed by rdma::communication::rdma_receiver.
 *
 * Wraps an rdma_receiver instance and delegates all calls to it.
 * Incoming rdma_comm receive events are converted to limestone-internal
 * rdma_data_event / rdma_error_event before the user-supplied handler is invoked.
 */
class rdma_comm_receiver : public rdma_receiver_base {
public:
    /**
     * @brief Construct with a pre-built rdma_config.
     * @param config Configuration for the underlying rdma_receiver.
     */
    explicit rdma_comm_receiver(rdma::communication::rdma_config config);

    ~rdma_comm_receiver() override = default;

    rdma_comm_receiver(rdma_comm_receiver const&) = delete;
    rdma_comm_receiver& operator=(rdma_comm_receiver const&) = delete;
    rdma_comm_receiver(rdma_comm_receiver&&) = delete;
    rdma_comm_receiver& operator=(rdma_comm_receiver&&) = delete;

    [[nodiscard]] operation_result initialize(rdma_receive_handler handler) noexcept override;
    [[nodiscard]] operation_result shutdown() noexcept override;

    [[nodiscard]] operation_result register_channel(
        std::uint16_t channel_id,
        int           ack_socket) noexcept override;

    [[nodiscard]] std::optional<std::uint64_t> get_dma_address() const noexcept override;

private:
    rdma::communication::rdma_receiver receiver_;
};

} // namespace limestone::replication
