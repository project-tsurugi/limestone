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

#include <rdma/rdma_sender_base.h>

namespace limestone::replication {

/**
 * @brief Null implementation of rdma_sender_base.
 *
 * initialize() and get_send_stream() return failure results because RDMA is not
 * available in this build configuration.
 * shutdown() succeeds without performing any RDMA operations.
 */
class null_rdma_sender : public rdma_sender_base {
public:
    null_rdma_sender() = default;
    ~null_rdma_sender() override = default;

    null_rdma_sender(null_rdma_sender const&) = delete;
    null_rdma_sender& operator=(null_rdma_sender const&) = delete;
    null_rdma_sender(null_rdma_sender&&) = delete;
    null_rdma_sender& operator=(null_rdma_sender&&) = delete;

    [[nodiscard]] operation_result initialize(std::uint64_t remote_dma_address) noexcept override;
    [[nodiscard]] stream_acquire_result get_send_stream(std::uint16_t channel_id, int ack_fd) noexcept override;
    [[nodiscard]] operation_result shutdown() noexcept override;
};

} // namespace limestone::replication
