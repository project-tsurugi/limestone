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
#include <memory>

#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_sender_base.h>

namespace limestone::replication {

/**
 * @brief Creates an rdma_sender_base instance appropriate for this build.
 *
 * Returns rdma_comm_sender when built with ENABLE_RDMA=ON,
 * or null_rdma_sender when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created sender instance.
 */
std::unique_ptr<rdma_sender_base> make_rdma_sender(std::uint32_t slot_count);

/**
 * @brief Creates an rdma_receiver_base instance appropriate for this build.
 *
 * Returns rdma_comm_receiver when built with ENABLE_RDMA=ON,
 * or null_rdma_receiver when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created receiver instance.
 */
std::unique_ptr<rdma_receiver_base> make_rdma_receiver(std::uint32_t slot_count);

} // namespace limestone::replication
