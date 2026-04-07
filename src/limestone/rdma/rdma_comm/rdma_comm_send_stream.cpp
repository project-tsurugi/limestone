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
#include <rdma/rdma_comm_send_stream.h>

namespace limestone::replication {

rdma_comm_send_stream::rdma_comm_send_stream(
        std::unique_ptr<rdma::communication::rdma_send_stream> stream)
    : stream_(std::move(stream))
{}

rdma_send_stream_base::send_result rdma_comm_send_stream::send_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept {
    auto r = stream_->send_bytes(payload, offset, length);
    return {r.success, r.error_message, r.bytes_written};
}

rdma_send_stream_base::send_result rdma_comm_send_stream::send_all_bytes(
        std::vector<std::uint8_t> const& payload,
        std::size_t offset,
        std::size_t length) noexcept {
    auto r = stream_->send_all_bytes(payload, offset, length);
    return {r.success, r.error_message, r.bytes_written};
}

rdma_send_stream_base::flush_result rdma_comm_send_stream::flush(
        std::chrono::milliseconds timeout) noexcept {
    auto r = stream_->flush(timeout);
    return {r.success, r.error_message};
}

} // namespace limestone::replication
