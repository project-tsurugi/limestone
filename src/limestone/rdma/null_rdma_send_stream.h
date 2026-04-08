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

#include <rdma/rdma_send_stream_base.h>

namespace limestone::replication {

/**
 * @brief Null implementation of rdma_send_stream_base.
 *
 * All send operations succeed immediately without transferring any data.
 * Used when RDMA is disabled at build time or runtime.
 */
class null_rdma_send_stream : public rdma_send_stream_base {
public:
    null_rdma_send_stream() = default;
    ~null_rdma_send_stream() override = default;

    null_rdma_send_stream(null_rdma_send_stream const&) = delete;
    null_rdma_send_stream& operator=(null_rdma_send_stream const&) = delete;
    null_rdma_send_stream(null_rdma_send_stream&&) = delete;
    null_rdma_send_stream& operator=(null_rdma_send_stream&&) = delete;

    [[nodiscard]] send_result send_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept override;

    [[nodiscard]] send_result send_all_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept override;

    [[nodiscard]] flush_result flush(std::chrono::milliseconds timeout) noexcept override;
};

} // namespace limestone::replication
