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

#include "rdma/rdma_receiver_base.h"
#include "rdma/rdma_sender_base.h"

namespace limestone::testing {

/**
 * @brief Minimal rdma_receiver_base stand-in for tests that must avoid the
 *        vendor RDMA mock.
 *
 * Returns success on every lifecycle call and exposes a fixed dummy DMA
 * address. Useful when a test needs the replica-side RDMA stack to be
 * "present and well-behaved" without actually engaging the vendor mock
 * (which is a process-wide singleton and conflicts with the leader-side
 * stack when both run in the same process).
 */
class noop_rdma_receiver : public limestone::replication::rdma_receiver_base {
public:
    // Arbitrary sentinel returned by get_dma_address(). The value is not a real DMA
    // address; it just needs to be non-zero and recognizable in test logs so that a
    // misuse (production path treating this stub as a real receiver) is easy to spot.
    static constexpr std::uint64_t dummy_dma_address = 0xCAFEBABEULL;

    operation_result initialize(
            limestone::replication::rdma_receive_handler /*handler*/) noexcept override {
        return {true, {}};
    }
    operation_result shutdown() noexcept override { return {true, {}}; }
    std::optional<std::uint64_t> get_dma_address() const noexcept override {
        return dummy_dma_address;
    }
    operation_result finalize_channel_setup_with_sender(
            limestone::replication::rdma_sender_base* /*sender*/) noexcept override {
        return {true, {}};
    }
};

/**
 * @brief Minimal rdma_sender_base stand-in for tests that must avoid the
 *        vendor RDMA mock.
 *
 * Symmetric counterpart to noop_rdma_receiver. get_send_stream() returns
 * a failure result because no test that uses this stub exercises the
 * data send path.
 */
class noop_rdma_sender : public limestone::replication::rdma_sender_base {
public:
    operation_result initialize(std::uint64_t /*remote_dma_address*/) noexcept override {
        return {true, {}};
    }
    stream_acquire_result get_send_stream(std::uint16_t /*channel_id*/) noexcept override {
        return {{false, "noop_rdma_sender does not support get_send_stream"}, nullptr};
    }
    operation_result finalize_channel_setup() noexcept override { return {true, {}}; }
    operation_result shutdown() noexcept override { return {true, {}}; }
};

}  // namespace limestone::testing
