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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace limestone::replication {

/**
 * @brief Abstract interface for an RDMA send stream.
 *
 * Provides the minimal API used by limestone for sending data via RDMA,
 * independent of the rdma_comm library.  Null and rdma_comm-backed
 * implementations live in the same rdma/ directory.
 */
class rdma_send_stream_base {
public:
    /**
     * @brief Result of a send_bytes / send_all_bytes call.
     */
    struct send_result {
        bool        success{};          ///< true if the operation completed without error.
        std::string error_message;      ///< Diagnostic text when success is false.
        std::size_t bytes_written{};    ///< Number of bytes actually transferred.
    };

    /**
     * @brief Result of a flush call.
     */
    struct flush_result {
        bool        success{};       ///< true if all pending acks were received.
        std::string error_message;   ///< Diagnostic text when success is false.
    };

    rdma_send_stream_base() = default;

    rdma_send_stream_base(rdma_send_stream_base const&) = delete;
    rdma_send_stream_base& operator=(rdma_send_stream_base const&) = delete;
    rdma_send_stream_base(rdma_send_stream_base&&) = delete;
    rdma_send_stream_base& operator=(rdma_send_stream_base&&) = delete;

    virtual ~rdma_send_stream_base() = default;

    /**
     * @brief Transfer a subset of payload starting at the given offset.
     * @param payload Byte sequence to send.
     * @param offset  Starting offset within payload.
     * @param length  Maximum number of bytes to transfer.
     * @return send_result describing status and bytes written.
     */
    [[nodiscard]] virtual send_result send_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept = 0;

    /**
     * @brief Transfer all requested bytes, retrying until complete or failure.
     * @param payload Byte sequence to send.
     * @param offset  Starting offset within payload.
     * @param length  Maximum number of bytes to transfer.
     * @return send_result describing status and bytes written.
     */
    [[nodiscard]] virtual send_result send_all_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept = 0;

    /**
     * @brief Wait until all outstanding acknowledgements are received.
     * @param timeout Maximum duration to wait.
     * @return flush_result describing success or failure.
     */
    [[nodiscard]] virtual flush_result flush(std::chrono::milliseconds timeout) noexcept = 0;
};

} // namespace limestone::replication
