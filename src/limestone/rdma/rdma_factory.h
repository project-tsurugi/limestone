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
 * @brief Creates a data-send rdma_sender_base instance (master side, data_only buffer).
 *
 * Returns rdma_comm_sender when built with ENABLE_RDMA=ON,
 * or null_rdma_sender when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created sender instance configured for data-only transmission.
 */
std::unique_ptr<rdma_sender_base> make_rdma_data_sender(std::uint32_t slot_count);

/**
 * @brief Creates an ACK-send rdma_sender_base instance (replica side, ack_only buffer).
 *
 * Returns rdma_comm_sender when built with ENABLE_RDMA=ON,
 * or null_rdma_sender when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created sender instance configured for ACK-only transmission.
 */
std::unique_ptr<rdma_sender_base> make_rdma_ack_sender(std::uint32_t slot_count);

/**
 * @brief Creates a data-receive rdma_receiver_base instance (replica side, data_only buffer).
 *
 * Returns rdma_comm_receiver when built with ENABLE_RDMA=ON,
 * or null_rdma_receiver when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created receiver instance configured for data-only reception.
 */
std::unique_ptr<rdma_receiver_base> make_rdma_data_receiver(std::uint32_t slot_count);

/**
 * @brief Creates an ACK-receive rdma_receiver_base instance (master side, ack_only buffer).
 *
 * Returns rdma_comm_receiver when built with ENABLE_RDMA=ON,
 * or null_rdma_receiver when built with ENABLE_RDMA=OFF.
 *
 * @param slot_count Number of RDMA slots (buffer capacity).
 * @return Newly created receiver instance configured for ACK-only reception.
 */
std::unique_ptr<rdma_receiver_base> make_rdma_ack_receiver(std::uint32_t slot_count);

} // namespace limestone::replication
