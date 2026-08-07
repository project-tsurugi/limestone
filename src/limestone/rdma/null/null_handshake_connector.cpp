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
#include <rdma/null_handshake_connector.h>

namespace limestone::replication {

handshake_connector_base::operation_result null_handshake_connector::start(
        std::uint64_t /*target_service*/,
        std::vector<std::uint8_t> const& /*start_payload*/) noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

handshake_connector_base::receive_result null_handshake_connector::receive_response() noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)", {}};
}

handshake_connector_base::operation_result null_handshake_connector::send_finalize(
        std::vector<std::uint8_t> const& /*finalize_payload*/) noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

handshake_connector_base::operation_result null_handshake_connector::send_ready() noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

handshake_connector_base::operation_result null_handshake_connector::receive_completion() noexcept {
    return {false, "RDMA is not enabled in this build (ENABLE_RDMA=OFF)"};
}

} // namespace limestone::replication
