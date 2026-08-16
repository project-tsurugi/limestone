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
#include <ostream>
#include <string>
#include <string_view>

namespace limestone::replication {

/**
 * @brief Replication transport mode.
 */
enum class replication_mode {
    none, ///< Replication is not configured.
    tcp,  ///< TCP replication addressed by a replication endpoint.
    rdma, ///< RDMA replication established via the handshake daemon.
};

/**
 * @brief Returns the string representation of a replication_mode value.
 * @param mode Value to convert.
 * @return String representation of the value.
 */
[[nodiscard]] std::string_view to_string_view(replication_mode mode) noexcept;

/**
 * @brief Writes the string representation of a replication_mode to the stream.
 * @param os Output stream.
 * @param mode Value to write.
 * @return The output stream.
 */
std::ostream& operator<<(std::ostream& os, replication_mode mode);

/**
 * @brief Validated replication settings.
 *
 * Pure value class: holds the selected replication mode and the settings the
 * mode requires. Instances are produced by a validating builder (see
 * replication_config_loader.h); an existing instance represents a consistent
 * combination of settings.
 */
class replication_config {
public:
    /**
     * @brief Creates a configuration with replication disabled (mode none).
     */
    replication_config() = default;

    /**
     * @brief Creates a configuration from validated values.
     * @param mode Selected replication mode.
     * @param handshake_socket_path UNIX domain socket path of the handshake
     *        daemon; empty unless mode is rdma.
     * @param service_id Handshake service_id shared by master and replica;
     *        0 unless mode is rdma.
     */
    replication_config(replication_mode mode, std::string handshake_socket_path,
        std::uint64_t service_id);

    /**
     * @brief Returns the selected replication mode.
     * @return Selected mode.
     */
    [[nodiscard]] replication_mode mode() const noexcept;

    /**
     * @brief Returns the UNIX domain socket path of the handshake daemon.
     * @return Socket path; empty unless mode() is rdma.
     */
    [[nodiscard]] std::string const& handshake_socket_path() const noexcept;

    /**
     * @brief Returns the handshake service_id shared by master and replica.
     * @return Service id; 0 unless mode() is rdma.
     */
    [[nodiscard]] std::uint64_t service_id() const noexcept;

private:
    replication_mode mode_{replication_mode::none};
    std::string handshake_socket_path_{};
    std::uint64_t service_id_{0};
};

/**
 * @brief Compares two configurations member-wise.
 * @param lhs Left-hand side.
 * @param rhs Right-hand side.
 * @return true when all members are equal.
 */
[[nodiscard]] bool operator==(
    replication_config const& lhs, replication_config const& rhs) noexcept;

/**
 * @brief Negation of operator==.
 * @param lhs Left-hand side.
 * @param rhs Right-hand side.
 * @return true when any member differs.
 */
[[nodiscard]] bool operator!=(
    replication_config const& lhs, replication_config const& rhs) noexcept;

/**
 * @brief Writes a human-readable representation of the configuration to the stream.
 * @param os Output stream.
 * @param config Value to write.
 * @return The output stream.
 */
std::ostream& operator<<(std::ostream& os, replication_config const& config);

} // namespace limestone::replication
