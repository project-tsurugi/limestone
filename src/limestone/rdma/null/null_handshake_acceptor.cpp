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
#include <rdma/null_handshake_acceptor.h>

namespace limestone::replication {

handshake_acceptor_base::receive_result null_handshake_acceptor::wait_for_start(
        std::uint64_t /*accepted_service*/) noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)", {}};
}

handshake_acceptor_base::operation_result null_handshake_acceptor::send_response(
        std::vector<std::uint8_t> const& /*response_payload*/) noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

handshake_acceptor_base::receive_result null_handshake_acceptor::receive_finalize() noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)", {}};
}

handshake_acceptor_base::operation_result null_handshake_acceptor::complete() noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

} // namespace limestone::replication
