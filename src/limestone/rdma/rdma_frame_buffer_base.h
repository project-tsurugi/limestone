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

namespace limestone::replication {

/**
 * @brief Abstract handle to a writable RDMA send frame.
 *
 * Acquired from rdma_send_stream_base::acquire_frame_buffer(). The caller writes
 * payload bytes directly into the region returned by payload() (up to capacity()
 * bytes) and then hands the handle to rdma_send_stream_base::submit_frame_buffer().
 * Writing into this region is the zero-copy path: no intermediate buffer is needed.
 *
 * Destroying a handle that was never submitted releases the underlying send-buffer
 * slots back to the pool, so a caller that fails midway may simply let the handle
 * go out of scope.
 */
class rdma_frame_buffer_base {
public:
    rdma_frame_buffer_base() = default;

    rdma_frame_buffer_base(rdma_frame_buffer_base const&) = delete;
    rdma_frame_buffer_base& operator=(rdma_frame_buffer_base const&) = delete;
    rdma_frame_buffer_base(rdma_frame_buffer_base&&) = delete;
    rdma_frame_buffer_base& operator=(rdma_frame_buffer_base&&) = delete;

    virtual ~rdma_frame_buffer_base() = default;

    /**
     * @brief Pointer to the writable payload region.
     * @return Beginning of the region the caller may fill, or nullptr when invalid.
     */
    [[nodiscard]] virtual std::uint8_t* payload() noexcept = 0;

    /**
     * @brief Number of bytes the caller may write into payload().
     * @return Writable capacity in bytes.
     */
    [[nodiscard]] virtual std::size_t capacity() const noexcept = 0;
};

} // namespace limestone::replication
