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
#include <rdma/rdma_comm_handshake_acceptor.h>

#include <utility>

#include <rdma/rdma_comm/rdma_comm_handshake_result_conversion.h>

namespace limestone::replication {

rdma_comm_handshake_acceptor::rdma_comm_handshake_acceptor(
        std::unique_ptr<rdma::handshake::handshake_acceptor> acceptor)
    : acceptor_(std::move(acceptor))
{}

handshake_client_base::receive_result rdma_comm_handshake_acceptor::wait_for_start(
        std::uint64_t accepted_service) noexcept {
    return to_receive_result(acceptor_->wait_for_start(accepted_service));
}

handshake_client_base::operation_result rdma_comm_handshake_acceptor::send_response(
        std::vector<std::uint8_t> const& response_payload) noexcept {
    return to_operation_result(acceptor_->send_response(response_payload));
}

handshake_client_base::receive_result rdma_comm_handshake_acceptor::receive_finalize() noexcept {
    return to_receive_result(acceptor_->receive_finalize());
}

handshake_client_base::operation_result rdma_comm_handshake_acceptor::complete() noexcept {
    return to_operation_result(acceptor_->complete());
}

} // namespace limestone::replication
