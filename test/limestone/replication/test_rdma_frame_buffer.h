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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <rdma/rdma_frame_buffer_base.h>

namespace limestone::testing {

/**
 * @brief Slot size the fake streams round their grants up to, mirroring the send ring.
 *
 * The real ring hands out whole slots, so a request for a few bytes comes back with a
 * whole slot's worth of capacity. Reproducing that here keeps callers honest: one that
 * writes capacity() bytes instead of clamping to what it actually has will overrun, and
 * the test will catch it.
 */
inline constexpr std::size_t test_rdma_slot_size = 4096U;

/**
 * @brief Round a requested payload size up to a whole slot, as the send ring does.
 * @param max_payload Bytes the caller asked for.
 * @return Capacity to grant: the smallest multiple of test_rdma_slot_size that fits.
 */
[[nodiscard]] inline std::size_t granted_frame_capacity(std::size_t max_payload) {
    auto const slots = (max_payload + test_rdma_slot_size - 1U) / test_rdma_slot_size;
    return slots * test_rdma_slot_size;
}

/**
 * @brief Heap-backed rdma_frame_buffer_base for tests.
 *
 * Stands in for a slice of the RDMA send ring: the stream under test hands one of
 * these to its caller, the caller writes into payload(), and the fake stream reads
 * the bytes back out via take_written() to assert on what would have been sent.
 */
class test_rdma_frame_buffer : public limestone::replication::rdma_frame_buffer_base {
public:
    /**
     * @brief Allocate a writable region of the given size.
     * @param capacity Number of bytes the caller may write.
     */
    explicit test_rdma_frame_buffer(std::size_t capacity) : storage_(capacity) {}

    ~test_rdma_frame_buffer() override = default;

    test_rdma_frame_buffer(test_rdma_frame_buffer const&) = delete;
    test_rdma_frame_buffer& operator=(test_rdma_frame_buffer const&) = delete;
    test_rdma_frame_buffer(test_rdma_frame_buffer&&) = delete;
    test_rdma_frame_buffer& operator=(test_rdma_frame_buffer&&) = delete;

    [[nodiscard]] std::uint8_t* payload() noexcept override { return storage_.data(); }

    [[nodiscard]] std::size_t capacity() const noexcept override { return storage_.size(); }

    /**
     * @brief Extract the first @p payload_size bytes the caller wrote.
     * @param payload_size Number of bytes the caller reported writing.
     * @return The written prefix of the region.
     */
    [[nodiscard]] std::vector<std::uint8_t> take_written(std::size_t payload_size) {
        EXPECT_LE(payload_size, storage_.size())
            << "payload_size exceeds the frame capacity";
        storage_.resize(std::min(payload_size, storage_.size()));
        return std::move(storage_);
    }

private:
    std::vector<std::uint8_t> storage_;
};

}  // namespace limestone::testing
