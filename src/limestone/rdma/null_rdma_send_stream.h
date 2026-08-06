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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <rdma/rdma_frame_buffer_base.h>
#include <rdma/rdma_send_stream_base.h>

namespace limestone::replication {

/**
 * @brief Null implementation of rdma_frame_buffer_base.
 *
 * Hands out an ordinary heap buffer so that a caller writing into payload() stays
 * within bounds. Whatever is written is discarded on submit.
 */
class null_rdma_frame_buffer : public rdma_frame_buffer_base {
public:
    /**
     * @brief Allocate a writable region of the requested size.
     * @param capacity Number of bytes the caller may write.
     */
    explicit null_rdma_frame_buffer(std::size_t capacity) : storage_(capacity) {}

    ~null_rdma_frame_buffer() override = default;

    null_rdma_frame_buffer(null_rdma_frame_buffer const&) = delete;
    null_rdma_frame_buffer& operator=(null_rdma_frame_buffer const&) = delete;
    null_rdma_frame_buffer(null_rdma_frame_buffer&&) = delete;
    null_rdma_frame_buffer& operator=(null_rdma_frame_buffer&&) = delete;

    [[nodiscard]] std::uint8_t* payload() noexcept override { return storage_.data(); }

    [[nodiscard]] std::size_t capacity() const noexcept override { return storage_.size(); }

private:
    std::vector<std::uint8_t> storage_;
};

/**
 * @brief Null implementation of rdma_send_stream_base.
 *
 * All send operations succeed immediately and discard the payload. Used when RDMA
 * is disabled at build time.
 *
 * @note Nothing constructs this in an ENABLE_RDMA=OFF build: null_rdma_sender's
 *       get_send_stream() always fails, so callers never obtain a stream. It exists
 *       to keep the base interface complete, and still behaves safely if called.
 */
class null_rdma_send_stream : public rdma_send_stream_base {
public:
    null_rdma_send_stream() = default;
    ~null_rdma_send_stream() override = default;

    null_rdma_send_stream(null_rdma_send_stream const&) = delete;
    null_rdma_send_stream& operator=(null_rdma_send_stream const&) = delete;
    null_rdma_send_stream(null_rdma_send_stream&&) = delete;
    null_rdma_send_stream& operator=(null_rdma_send_stream&&) = delete;

    [[nodiscard]] std::unique_ptr<rdma_frame_buffer_base> acquire_frame_buffer(
        std::size_t max_payload,
        std::size_t min_capacity) noexcept override;

    [[nodiscard]] send_result submit_frame_buffer(
        rdma_frame_buffer_base& frame,
        std::size_t             payload_size) override;

    [[nodiscard]] flush_result flush(std::chrono::milliseconds timeout) noexcept override;
};

} // namespace limestone::replication
