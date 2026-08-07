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

#include <memory>

#include <rdma_comm/handshake/handshake_acceptor.h>

#include <rdma/handshake_client_base.h>

namespace limestone::replication {

/**
 * @brief handshake_acceptor_base implementation backed by rdma::handshake::handshake_acceptor.
 *
 * Wraps an rdma::handshake::handshake_acceptor instance and delegates all calls to it,
 * converting rdma-comm-lib result types to the limestone-internal equivalents.
 */
class rdma_comm_handshake_acceptor : public handshake_acceptor_base {
public:
    /**
     * @brief Construct with a pre-created rdma-comm-lib handshake_acceptor instance.
     * @param acceptor Underlying acceptor instance; must not be null.
     */
    explicit rdma_comm_handshake_acceptor(
        std::unique_ptr<rdma::handshake::handshake_acceptor> acceptor);

    ~rdma_comm_handshake_acceptor() override = default;

    rdma_comm_handshake_acceptor(rdma_comm_handshake_acceptor const&) = delete;
    rdma_comm_handshake_acceptor& operator=(rdma_comm_handshake_acceptor const&) = delete;
    rdma_comm_handshake_acceptor(rdma_comm_handshake_acceptor&&) = delete;
    rdma_comm_handshake_acceptor& operator=(rdma_comm_handshake_acceptor&&) = delete;

    [[nodiscard]] receive_result wait_for_start(std::uint64_t accepted_service) noexcept override;

    [[nodiscard]] operation_result send_response(
        std::vector<std::uint8_t> const& response_payload) noexcept override;

    [[nodiscard]] receive_result receive_finalize() noexcept override;

    [[nodiscard]] operation_result complete() noexcept override;

private:
    std::unique_ptr<rdma::handshake::handshake_acceptor> acceptor_;
};

} // namespace limestone::replication
