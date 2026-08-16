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
#include <rdma/rdma_comm_handshake_connector.h>

#include <utility>

#include <rdma/rdma_comm/rdma_comm_handshake_result_conversion.h>

namespace limestone::replication {

rdma_comm_handshake_connector::rdma_comm_handshake_connector(
        std::unique_ptr<rdma::handshake::handshake_connector> connector)
    : connector_(std::move(connector))
{}

handshake_client_base::operation_result rdma_comm_handshake_connector::start(
        std::uint64_t                    target_service,
        std::vector<std::uint8_t> const& start_payload) noexcept {
    return to_operation_result(connector_->start(target_service, start_payload));
}

handshake_client_base::receive_result rdma_comm_handshake_connector::receive_response() noexcept {
    return to_receive_result(connector_->receive_response());
}

handshake_client_base::operation_result rdma_comm_handshake_connector::send_finalize(
        std::vector<std::uint8_t> const& finalize_payload) noexcept {
    return to_operation_result(connector_->send_finalize(finalize_payload));
}

handshake_client_base::operation_result rdma_comm_handshake_connector::send_ready() noexcept {
    return to_operation_result(connector_->send_ready());
}

handshake_client_base::operation_result
rdma_comm_handshake_connector::receive_completion() noexcept {
    return to_operation_result(connector_->receive_completion());
}

} // namespace limestone::replication
