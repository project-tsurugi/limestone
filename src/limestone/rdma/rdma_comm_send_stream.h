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

#include <rdma_comm/rdma_sender.h>

#include <rdma/rdma_send_stream_base.h>

namespace limestone::replication {

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

    [[nodiscard]] send_result send_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept override;

    [[nodiscard]] send_result send_all_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept override;

    [[nodiscard]] flush_result flush(std::chrono::milliseconds timeout) noexcept override;

private:
    std::unique_ptr<rdma::communication::rdma_send_stream> stream_;
};

} // namespace limestone::replication
