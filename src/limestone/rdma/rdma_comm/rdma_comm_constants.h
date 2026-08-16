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

#include <rdma_comm/constants.h>
#include <rdma_comm/rdma_frame_header.h>

namespace limestone::replication {

/**
 * @brief Size of one RDMA send-ring slot, in bytes.
 *
 * This is limestone's own configuration value, handed to rdma-comm-lib as
 * rdma_config::send_buffer.chunk_size_bytes by rdma_factory_rdma.cpp. It must equal
 * the vendor RDMA write granularity the library publishes as
 * dma_buffer_alignment_bytes; the static_assert below pins that down.
 */
inline constexpr std::size_t rdma_slot_size_bytes = 4096U;

static_assert(rdma_slot_size_bytes == rdma::communication::dma_buffer_alignment_bytes,
    "the ring slot size must match the vendor RDMA write granularity");

static_assert(rdma_slot_size_bytes <= rdma::communication::max_dma_write_bytes,
    "a single ring slot must fit within one vendor DMA write");

/**
 * @brief Largest min_capacity acquire_frame_buffer() can unconditionally satisfy.
 *
 * A valid frame always spans at least one ring slot, so this is the payload of one
 * slot after subtracting the frame header.
 */
inline constexpr std::size_t rdma_slot_payload_bytes =
    rdma_slot_size_bytes - static_cast<std::size_t>(rdma::communication::rdma_frame_header_size);

} // namespace limestone::replication
