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
#include <replication/replication_config_loader.h>

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <utility>

namespace limestone::replication {

namespace {

constexpr char const* handshake_socket_env_name = "TSURUGI_REPLICATION_HANDSHAKE_SOCKET";
constexpr char const* service_id_env_name = "TSURUGI_REPLICATION_SERVICE_ID";
constexpr char const* endpoint_env_name = "TSURUGI_REPLICATION_ENDPOINT";

replication_config_parse_result fail(std::string message) {
    return {false, std::move(message), replication_config{}};
}

replication_config_parse_result succeed(replication_config config) {
    return {true, {}, std::move(config)};
}

} // namespace

replication_config_parse_result make_replication_config(replication_config_input const& input) {
    if (input.handshake_socket.has_value() && input.endpoint_defined) {
        return fail("the handshake socket and the TCP endpoint are both configured; "
            "the replication mode is ambiguous");
    }
    if (!input.handshake_socket.has_value()) {
        if (input.service_id.has_value()) {
            return fail("a service_id is configured without a handshake socket; "
                "the RDMA replication settings are incomplete");
        }
        if (input.endpoint_defined) {
            return succeed(replication_config{replication_mode::tcp, {}, 0});
        }
        return succeed(replication_config{});
    }
#ifndef LIMESTONE_ENABLE_RDMA
    return fail("a handshake socket is configured but this build does not support RDMA "
        "(ENABLE_RDMA=OFF)");
#else
    if (input.handshake_socket->empty()) {
        return fail("the configured handshake socket path is empty");
    }
    if (!input.service_id.has_value()) {
        return fail("a service_id must be configured in RDMA replication mode");
    }
    // 0 and uint64 max are reserved by rdma-comm-lib and cannot be used.
    if (*input.service_id == 0
        || *input.service_id == std::numeric_limits<std::uint64_t>::max()) {
        return fail("the configured service_id is a reserved value (0 or uint64 max): "
            + std::to_string(*input.service_id));
    }
    return succeed(
        replication_config{replication_mode::rdma, *input.handshake_socket, *input.service_id});
#endif
}

std::optional<std::uint64_t> parse_service_id(std::string const& text) {
    if (text.empty()) {
        return std::nullopt;
    }
    if (text.find_first_not_of("0123456789") != std::string::npos) {
        return std::nullopt;
    }
    errno = 0;
    std::uint64_t const parsed = std::strtoull(text.c_str(), nullptr, 10);
    if (errno == ERANGE) {
        return std::nullopt;
    }
    return parsed;
}

replication_config_parse_result load_replication_config_from_environment() {
    replication_config_input input{};

    if (char const* socket_env = std::getenv(handshake_socket_env_name); socket_env != nullptr) {
        input.handshake_socket = std::string{socket_env};
    }
    if (char const* service_id_env = std::getenv(service_id_env_name); service_id_env != nullptr) {
        auto parsed = parse_service_id(service_id_env);
        if (!parsed.has_value()) {
            return fail(std::string(service_id_env_name)
                + " is not a valid unsigned 64-bit decimal integer: " + service_id_env);
        }
        input.service_id = parsed;
    }
    input.endpoint_defined = std::getenv(endpoint_env_name) != nullptr;

    return make_replication_config(input);
}

} // namespace limestone::replication
