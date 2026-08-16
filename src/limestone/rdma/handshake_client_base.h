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

#include <cstdint>
#include <string>
#include <vector>

namespace limestone::replication {

/**
 * @brief Common base of the RDMA handshake client interfaces.
 *
 * Wraps the rdma-comm-lib handshake client (DMA-address exchange via a local
 * handshake daemon) independently of the rdma-comm-lib headers. Null and
 * rdma_comm-backed implementations live in the same rdma/ directory.
 *
 * @note Implementations are not thread-safe.
 */
class handshake_client_base {
public:
    /**
     * @brief Result of a handshake operation that carries no payload.
     */
    struct operation_result {
        bool        success{};       ///< true if the operation succeeded.
        std::string error_message;   ///< Diagnostic text when success is false.
    };

    /**
     * @brief Result of a handshake operation that returns a payload from the peer.
     */
    struct receive_result {
        bool                      success{};       ///< true if the operation succeeded.
        std::string               error_message;   ///< Diagnostic text when success is false.
        std::vector<std::uint8_t> payload;         ///< Payload bytes; valid only when success.
    };

    handshake_client_base() = default;

    handshake_client_base(handshake_client_base const&) = delete;
    handshake_client_base& operator=(handshake_client_base const&) = delete;
    handshake_client_base(handshake_client_base&&) = delete;
    handshake_client_base& operator=(handshake_client_base&&) = delete;

    virtual ~handshake_client_base() = default;
};

/**
 * @brief Abstract interface for the connect side (initiator) of an RDMA handshake.
 *
 * @note The call sequence is start() -> receive_response() -> send_finalize() ->
 *       send_ready() -> receive_completion(), and must be followed in that order.
 */
class handshake_connector_base : public handshake_client_base {
public:
    /**
     * @brief Begin the handshake by sending the start payload to the peer.
     * @param target_service Identifier of the handshake service to connect to on the peer.
     * @param start_payload Opaque start payload.
     * @return operation_result describing success or failure.
     * @note Must be the first call on this instance.
     * @note On failure this instance cannot be reused; retrying requires a new instance.
     */
    [[nodiscard]] virtual operation_result start(
        std::uint64_t                    target_service,
        std::vector<std::uint8_t> const& start_payload) noexcept = 0;

    /**
     * @brief Receive the accept side's response payload.
     * @return receive_result with the response payload on success.
     * @note Must be called after start().
     */
    [[nodiscard]] virtual receive_result receive_response() noexcept = 0;

    /**
     * @brief Send the finalize payload to the peer.
     * @param finalize_payload Opaque finalize payload.
     * @return operation_result describing success or failure.
     * @note Must be called after a successful receive_response().
     */
    [[nodiscard]] virtual operation_result send_finalize(
        std::vector<std::uint8_t> const& finalize_payload) noexcept = 0;

    /**
     * @brief Send the message announcing local initialization is complete.
     * @return operation_result describing success or failure.
     * @note Must be called after send_finalize() and before receive_completion().
     */
    [[nodiscard]] virtual operation_result send_ready() noexcept = 0;

    /**
     * @brief Receive the completion response and finish the handshake.
     * @return operation_result describing success or failure.
     * @note Must be the last call, after send_ready().
     */
    [[nodiscard]] virtual operation_result receive_completion() noexcept = 0;
};

/**
 * @brief Abstract interface for the accept side (responder) of an RDMA handshake.
 *
 * @note The call sequence is wait_for_start() -> send_response() ->
 *       receive_finalize() -> complete(), and must be followed in that order.
 */
class handshake_acceptor_base : public handshake_client_base {
public:
    /**
     * @brief Wait for an incoming handshake and receive the connect side's start payload.
     * @param accepted_service Identifier of the handshake service to accept on.
     * @return receive_result with the start payload on success.
     * @note Must be the first call on this instance.
     */
    [[nodiscard]] virtual receive_result wait_for_start(
        std::uint64_t accepted_service) noexcept = 0;

    /**
     * @brief Send the response payload to the connect side.
     * @param response_payload Opaque response payload.
     * @return operation_result describing success or failure.
     * @note Must be called after a successful wait_for_start().
     */
    [[nodiscard]] virtual operation_result send_response(
        std::vector<std::uint8_t> const& response_payload) noexcept = 0;

    /**
     * @brief Receive the connect side's finalize payload.
     * @return receive_result with the finalize payload on success.
     * @note Must be called after send_response().
     */
    [[nodiscard]] virtual receive_result receive_finalize() noexcept = 0;

    /**
     * @brief Wait for the connect side's ready message and send the completion response.
     * @return operation_result describing success or failure.
     * @note Must be the last call, after receive_finalize().
     */
    [[nodiscard]] virtual operation_result complete() noexcept = 0;
};

} // namespace limestone::replication
