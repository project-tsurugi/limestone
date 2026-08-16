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
#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace limestone::replication {

/**
 * @brief Start payload the master sends on the RDMA handshake start step.
 */
struct rdma_handshake_start_payload {
    std::uint64_t protocol_version{};    ///< Replication protocol version.
    std::string   configuration_id;      ///< Configuration identity of the master.
    std::uint64_t epoch_number{};        ///< Epoch number at session start.
    std::uint32_t slot_count{};          ///< Ring capacity of the RDMA send buffer.
    std::uint64_t master_dma_address{};  ///< DMA address of the master's receiver.
    std::uint16_t channel_count{};       ///< Number of data channels (ids 0 .. count - 1).
    std::uint16_t control_channel_id{};  ///< Channel id reserved for control messages.
};

/**
 * @brief Response payload the replica returns on the RDMA handshake response step.
 */
struct rdma_handshake_response_payload {
    bool          accepted{};             ///< True when the replica accepted the session.
    std::string   error_message;          ///< Rejection reason; empty when accepted.
    std::uint64_t replica_dma_address{};  ///< DMA address of the replica's receiver.
};

/**
 * @brief Compares two start payloads field by field.
 * @param lhs Left-hand operand.
 * @param rhs Right-hand operand.
 * @return True when all fields are equal.
 */
[[nodiscard]] bool operator==(
    rdma_handshake_start_payload const& lhs, rdma_handshake_start_payload const& rhs);

/**
 * @brief Negation of operator==.
 * @param lhs Left-hand operand.
 * @param rhs Right-hand operand.
 * @return True when any field differs.
 */
[[nodiscard]] bool operator!=(
    rdma_handshake_start_payload const& lhs, rdma_handshake_start_payload const& rhs);

/**
 * @brief Writes a start payload to a stream for logging.
 * @param out Destination stream.
 * @param value Payload to write.
 * @return The stream.
 */
std::ostream& operator<<(std::ostream& out, rdma_handshake_start_payload const& value);

/**
 * @brief Compares two response payloads field by field.
 * @param lhs Left-hand operand.
 * @param rhs Right-hand operand.
 * @return True when all fields are equal.
 */
[[nodiscard]] bool operator==(
    rdma_handshake_response_payload const& lhs, rdma_handshake_response_payload const& rhs);

/**
 * @brief Negation of operator==.
 * @param lhs Left-hand operand.
 * @param rhs Right-hand operand.
 * @return True when any field differs.
 */
[[nodiscard]] bool operator!=(
    rdma_handshake_response_payload const& lhs, rdma_handshake_response_payload const& rhs);

/**
 * @brief Writes a response payload to a stream for logging.
 * @param out Destination stream.
 * @param value Payload to write.
 * @return The stream.
 */
std::ostream& operator<<(std::ostream& out, rdma_handshake_response_payload const& value);

/**
 * @brief Encodes a start payload into the handshake wire format.
 * @param payload Payload to encode.
 * @return Encoded bytes.
 */
[[nodiscard]] std::vector<std::uint8_t> encode(rdma_handshake_start_payload const& payload);

/**
 * @brief Encodes a response payload into the handshake wire format.
 * @param payload Payload to encode.
 * @return Encoded bytes.
 */
[[nodiscard]] std::vector<std::uint8_t> encode(rdma_handshake_response_payload const& payload);

/**
 * @brief Decodes a start payload from the handshake wire format.
 * @param bytes Encoded bytes.
 * @return Decoded payload, or std::nullopt when bytes are truncated, malformed, or carry
 *         trailing data.
 */
[[nodiscard]] std::optional<rdma_handshake_start_payload> decode_start_payload(
    std::vector<std::uint8_t> const& bytes);

/**
 * @brief Decodes a response payload from the handshake wire format.
 * @param bytes Encoded bytes.
 * @return Decoded payload, or std::nullopt when bytes are truncated, malformed, or carry
 *         trailing data.
 */
[[nodiscard]] std::optional<rdma_handshake_response_payload> decode_response_payload(
    std::vector<std::uint8_t> const& bytes);

} // namespace limestone::replication
