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
#include <rdma/null_rdma_sender.h>

namespace limestone::replication {

rdma_sender_base::operation_result null_rdma_sender::initialize(
        std::uint64_t /*remote_dma_address*/) noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

rdma_sender_base::stream_acquire_result null_rdma_sender::get_send_stream(
        std::uint16_t /*channel_id*/,
        int /*ack_fd*/) noexcept {
    return {{false, "RDMA is not enabled in this build"}, nullptr};
}

rdma_sender_base::operation_result null_rdma_sender::shutdown() noexcept {
    return {true, ""};
}

} // namespace limestone::replication
