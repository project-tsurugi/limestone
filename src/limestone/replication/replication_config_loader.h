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
#include <string>

#include <replication/replication_config.h>

namespace limestone::replication {

/**
 * @brief Source-independent, typed input for building a replication_config.
 */
struct replication_config_input {
    std::optional<std::string> handshake_socket{};   ///< UDS path of the handshake daemon.
    std::optional<std::uint64_t> service_id{};       ///< Handshake service_id.
    bool endpoint_defined{false};                    ///< Whether a TCP endpoint is configured.
};

/**
 * @brief Result of building a replication_config.
 */
struct replication_config_parse_result {
    bool ok{false};               ///< Outcome of the validation.
    std::string error_message{};  ///< Reason of the failure; empty when ok.
    replication_config config{};  ///< Built configuration; mode none when not ok.
};

/**
 * @brief Validates typed input values and builds a replication_config.
 *
 * Determines the replication mode:
 * - handshake_socket set: rdma mode. Requires a build with RDMA support, a
 *   non-empty socket path, and a service_id that is not a reserved value
 *   (0 and uint64 max are reserved by rdma-comm-lib).
 * - endpoint_defined: tcp mode.
 * - Neither: replication disabled.
 * - Ambiguous or incomplete combinations (handshake_socket together with
 *   endpoint_defined, or service_id without handshake_socket) fail.
 *
 * @param input Typed input values.
 * @return Result carrying the built configuration or the failure reason.
 */
[[nodiscard]] replication_config_parse_result make_replication_config(
    replication_config_input const& input);

/**
 * @brief Parses a service_id from its decimal string representation.
 *
 * Accepts decimal digits only: no sign, no whitespace, no trailing characters.
 *
 * @param text String to parse.
 * @return Parsed value, or std::nullopt when the string is not a valid
 *         unsigned 64-bit decimal integer.
 */
[[nodiscard]] std::optional<std::uint64_t> parse_service_id(std::string const& text);

/**
 * @brief Builds a replication_config from the environment variables.
 *
 * Reads TSURUGI_REPLICATION_HANDSHAKE_SOCKET, TSURUGI_REPLICATION_SERVICE_ID
 * and TSURUGI_REPLICATION_ENDPOINT, then delegates the validation to
 * make_replication_config().
 *
 * @return Result carrying the built configuration or the failure reason.
 */
[[nodiscard]] replication_config_parse_result load_replication_config_from_environment();

} // namespace limestone::replication
