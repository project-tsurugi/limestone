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
#include <replication/replication_config.h>

#include <utility>

namespace limestone::replication {

replication_config::replication_config(replication_mode mode, std::string handshake_socket_path,
    std::uint64_t service_id)
    : mode_(mode)
    , handshake_socket_path_(std::move(handshake_socket_path))
    , service_id_(service_id) {
}

replication_mode replication_config::mode() const noexcept {
    return mode_;
}

std::string const& replication_config::handshake_socket_path() const noexcept {
    return handshake_socket_path_;
}

std::uint64_t replication_config::service_id() const noexcept {
    return service_id_;
}

bool operator==(replication_config const& lhs, replication_config const& rhs) noexcept {
    return lhs.mode() == rhs.mode()
        && lhs.handshake_socket_path() == rhs.handshake_socket_path()
        && lhs.service_id() == rhs.service_id();
}

bool operator!=(replication_config const& lhs, replication_config const& rhs) noexcept {
    return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os, replication_config const& config) {
    os << "replication_config{mode=" << config.mode();
    if (config.mode() == replication_mode::rdma) {
        os << ", handshake_socket_path=" << config.handshake_socket_path()
           << ", service_id=" << config.service_id();
    }
    return os << "}";
}

std::string_view to_string_view(replication_mode mode) noexcept {
    switch (mode) {
        case replication_mode::none: return "none";
        case replication_mode::tcp: return "tcp";
        case replication_mode::rdma: return "rdma";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& os, replication_mode mode) {
    return os << to_string_view(mode);
}

} // namespace limestone::replication
