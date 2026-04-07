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
#include <rdma/rdma_comm_sender.h>
#include <rdma/rdma_comm_send_stream.h>

#include <rdma_comm/unique_fd.h>

namespace limestone::replication {

rdma_comm_sender::rdma_comm_sender(rdma::communication::rdma_config config)
    : sender_(std::move(config))
{}

rdma_sender_base::operation_result rdma_comm_sender::initialize(
        std::uint64_t remote_dma_address) noexcept {
    auto r = sender_.initialize(remote_dma_address);
    return {r.success, r.error_message};
}

rdma_sender_base::stream_acquire_result rdma_comm_sender::get_send_stream(
        std::uint16_t channel_id,
        int           ack_fd) noexcept {
    auto r = sender_.get_send_stream(
        channel_id,
        rdma::communication::unique_fd{ack_fd});
    if (! r.status.success || ! r.stream) {
        return {{false, r.status.error_message}, nullptr};
    }
    auto wrapped = std::make_unique<rdma_comm_send_stream>(std::move(r.stream));
    return {{true, ""}, std::move(wrapped)};
}

rdma_sender_base::operation_result rdma_comm_sender::shutdown() noexcept {
    auto r = sender_.shutdown();
    return {r.success, r.error_message};
}

} // namespace limestone::replication
