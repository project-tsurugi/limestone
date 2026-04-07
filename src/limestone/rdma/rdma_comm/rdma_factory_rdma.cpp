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
#include <rdma/rdma_factory.h>
#include <rdma/rdma_comm_receiver.h>
#include <rdma/rdma_comm_sender.h>

#include <rdma_comm/rdma_config.h>

namespace limestone::replication {

std::unique_ptr<rdma_sender_base> make_rdma_sender(std::uint32_t slot_count) {
    rdma::communication::rdma_config config{};
    auto capacity = static_cast<std::size_t>(slot_count);
    constexpr std::size_t chunk_size = 4096U;
    config.send_buffer.region_size_bytes = capacity * chunk_size;
    config.send_buffer.chunk_size_bytes = chunk_size;
    config.send_buffer.ring_capacity = capacity;
    config.remote_buffer = config.send_buffer;
    config.completion_queue_depth = 1024U;
    config.write_log_mode = rdma::communication::rdma_write_log_mode::full;
    return std::make_unique<rdma_comm_sender>(std::move(config));
}

std::unique_ptr<rdma_receiver_base> make_rdma_receiver(std::uint32_t slot_count) {
    rdma::communication::rdma_config config{};
    auto capacity = static_cast<std::size_t>(slot_count);
    constexpr std::size_t chunk_size = 4096U;
    config.send_buffer.region_size_bytes = capacity * chunk_size;
    config.send_buffer.chunk_size_bytes = chunk_size;
    config.send_buffer.ring_capacity = capacity;
    config.remote_buffer = config.send_buffer;
    config.completion_queue_depth = 1024U;
    return std::make_unique<rdma_comm_receiver>(std::move(config));
}

} // namespace limestone::replication
