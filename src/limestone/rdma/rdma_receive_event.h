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

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace limestone::replication {

/// @brief RDMA frame protocol version constant (mirrors rdma_comm::rdma_frame_protocol_version).
inline constexpr std::uint8_t rdma_frame_current_version = 1U;

/// @brief Flag indicating that a frame carries a partial payload (mirrors rdma_comm).
inline constexpr std::uint8_t rdma_frame_flag_partial_payload = 0x02U;

/**
 * @brief Limestone-internal RDMA frame header.
 *
 * Field layout mirrors rdma::communication::rdma_frame_header so that the
 * rdma_comm wrapper can copy fields directly.
 */
struct rdma_frame_header {
    std::uint8_t  version{};          ///< Frame format version.
    std::uint8_t  flags{};            ///< Bitmask of control flags.
    std::uint16_t sequence_number{};  ///< Per-channel wrap-around sequence number.
    std::uint16_t channel_id{};       ///< Logical channel identifier.
    std::uint32_t payload_size{};     ///< Number of payload bytes that follow.
};

/**
 * @brief Limestone-internal RDMA data receive event.
 *
 * Carries a successfully received frame and its payload.
 */
struct rdma_data_event {
    rdma_frame_header       header{};
    std::vector<std::uint8_t> payload;
};

/**
 * @brief Limestone-internal RDMA error receive event.
 *
 * Describes an error condition observed by the receiver.
 */
struct rdma_error_event {
    std::optional<rdma_frame_header> header{};
    std::vector<std::uint8_t>        payload;
    std::string                      error_message;
};

/// @brief Variant combining both event types delivered by the RDMA receiver.
using rdma_receive_event = std::variant<rdma_data_event, rdma_error_event>;

} // namespace limestone::replication
