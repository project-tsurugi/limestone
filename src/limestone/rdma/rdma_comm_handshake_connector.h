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

#include <rdma_comm/handshake/handshake_connector.h>

#include <rdma/handshake_client_base.h>

namespace limestone::replication {

/**
 * @brief handshake_connector_base implementation backed by rdma::handshake::handshake_connector.
 *
 * Wraps an rdma::handshake::handshake_connector instance and delegates all calls to it,
 * converting rdma-comm-lib result types to the limestone-internal equivalents.
 */
class rdma_comm_handshake_connector : public handshake_connector_base {
public:
    /**
     * @brief Construct with a pre-created rdma-comm-lib handshake_connector instance.
     * @param connector Underlying connector instance; must not be null.
     */
    explicit rdma_comm_handshake_connector(
        std::unique_ptr<rdma::handshake::handshake_connector> connector);

    ~rdma_comm_handshake_connector() override = default;

    rdma_comm_handshake_connector(rdma_comm_handshake_connector const&) = delete;
    rdma_comm_handshake_connector& operator=(rdma_comm_handshake_connector const&) = delete;
    rdma_comm_handshake_connector(rdma_comm_handshake_connector&&) = delete;
    rdma_comm_handshake_connector& operator=(rdma_comm_handshake_connector&&) = delete;

    [[nodiscard]] operation_result start(
        std::uint64_t                    target_service,
        std::vector<std::uint8_t> const& start_payload) noexcept override;

    [[nodiscard]] receive_result receive_response() noexcept override;

    [[nodiscard]] operation_result send_finalize(
        std::vector<std::uint8_t> const& finalize_payload) noexcept override;

    [[nodiscard]] operation_result send_ready() noexcept override;

    [[nodiscard]] operation_result receive_completion() noexcept override;

private:
    std::unique_ptr<rdma::handshake::handshake_connector> connector_;
};

} // namespace limestone::replication
