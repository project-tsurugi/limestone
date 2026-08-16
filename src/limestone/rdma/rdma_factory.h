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

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <rdma/handshake_client_base.h>
#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_sender_base.h>

namespace limestone::replication {

/**
 * @brief Result of creating a handshake_connector_base instance.
 */
struct handshake_connector_create_result {
    handshake_client_base::operation_result   status;   ///< Outcome of the creation.
    std::unique_ptr<handshake_connector_base> instance; ///< Created instance (null on failure).
};

/**
 * @brief Result of creating a handshake_acceptor_base instance.
 */
struct handshake_acceptor_create_result {
    handshake_client_base::operation_result  status;   ///< Outcome of the creation.
    std::unique_ptr<handshake_acceptor_base> instance; ///< Created instance (null on failure).
};

/**
 * @brief Creates a data-send rdma_sender_base instance (master side, data_only buffer).
 *
 * Returns rdma_comm_sender when built with ENABLE_RDMA=ON,
 * or null_rdma_sender when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created sender instance configured for data-only transmission.
 */
std::unique_ptr<rdma_sender_base> make_rdma_data_sender(std::uint32_t slot_count);

/**
 * @brief Creates an ACK-send rdma_sender_base instance (replica side, ack_only buffer).
 *
 * Returns rdma_comm_sender when built with ENABLE_RDMA=ON,
 * or null_rdma_sender when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created sender instance configured for ACK-only transmission.
 */
std::unique_ptr<rdma_sender_base> make_rdma_ack_sender(std::uint32_t slot_count);

/**
 * @brief Creates a data-receive rdma_receiver_base instance (replica side, data_only buffer).
 *
 * Returns rdma_comm_receiver when built with ENABLE_RDMA=ON,
 * or null_rdma_receiver when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created receiver instance configured for data-only reception.
 */
std::unique_ptr<rdma_receiver_base> make_rdma_data_receiver(std::uint32_t slot_count);

/**
 * @brief Creates an ACK-receive rdma_receiver_base instance (master side, ack_only buffer).
 *
 * Returns rdma_comm_receiver when built with ENABLE_RDMA=ON,
 * or null_rdma_receiver when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created receiver instance configured for ACK-only reception.
 */
std::unique_ptr<rdma_receiver_base> make_rdma_ack_receiver(std::uint32_t slot_count);

/**
 * @brief Creates a connect-side handshake_connector_base instance bound to a local
 *        handshake daemon.
 *
 * Returns rdma_comm_handshake_connector when built with ENABLE_RDMA=ON,
 * or null_handshake_connector when built with ENABLE_RDMA=OFF.
 *
 * @param daemon_socket_path Filesystem path of the daemon's UNIX domain socket.
 * @param operation_timeout Upper bound a blocking call on the created instance waits
 *        for its message exchange to complete.
 * @return handshake_connector_create_result; instance is non-null on success.
 */
handshake_connector_create_result make_handshake_connector(
    std::string const&        daemon_socket_path,
    std::chrono::milliseconds operation_timeout);

/**
 * @brief Creates an accept-side handshake_acceptor_base instance bound to a local
 *        handshake daemon.
 *
 * Returns rdma_comm_handshake_acceptor when built with ENABLE_RDMA=ON,
 * or null_handshake_acceptor when built with ENABLE_RDMA=OFF.
 *
 * @param daemon_socket_path Filesystem path of the daemon's UNIX domain socket.
 * @param operation_timeout Upper bound a blocking call on the created instance waits
 *        for its message exchange to complete. wait_for_start's wait for the peer's
 *        start is excluded and bounded by start_wait_timeout.
 * @param start_wait_timeout Upper bound on how long wait_for_start waits for the
 *        peer's start; std::nullopt (the default) waits indefinitely.
 * @return handshake_acceptor_create_result; instance is non-null on success.
 */
handshake_acceptor_create_result make_handshake_acceptor(
    std::string const&        daemon_socket_path,
    std::chrono::milliseconds operation_timeout,
    std::optional<std::chrono::milliseconds> start_wait_timeout = std::nullopt);

} // namespace limestone::replication
