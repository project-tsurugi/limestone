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

#include <rdma/handshake_client_base.h>

namespace limestone::replication {

/**
 * @brief Null implementation of handshake_acceptor_base.
 *
 * Every call returns a failure result because RDMA is not available in this
 * build configuration.
 */
class null_handshake_acceptor : public handshake_acceptor_base {
public:
    null_handshake_acceptor() = default;
    ~null_handshake_acceptor() override = default;

    null_handshake_acceptor(null_handshake_acceptor const&) = delete;
    null_handshake_acceptor& operator=(null_handshake_acceptor const&) = delete;
    null_handshake_acceptor(null_handshake_acceptor&&) = delete;
    null_handshake_acceptor& operator=(null_handshake_acceptor&&) = delete;

    [[nodiscard]] receive_result wait_for_start(std::uint64_t accepted_service) noexcept override;
    [[nodiscard]] operation_result send_response(
        std::vector<std::uint8_t> const& response_payload) noexcept override;
    [[nodiscard]] receive_result receive_finalize() noexcept override;
    [[nodiscard]] operation_result complete() noexcept override;
};

} // namespace limestone::replication
