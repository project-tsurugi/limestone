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
#pragma once

#include <rdma_comm/handshake/handshake_types.h>
#include <rdma_comm/handshake/operation_result.h>

#include <rdma/handshake_client_base.h>

namespace limestone::replication {

/**
 * @brief Convert rdma::handshake::operation_result to the limestone-internal equivalent.
 * @param r Result to convert.
 * @return Converted operation_result.
 */
[[nodiscard]] handshake_client_base::operation_result to_operation_result(
    rdma::handshake::operation_result const& r);

/**
 * @brief Convert rdma::handshake::receive_result to the limestone-internal equivalent.
 * @param r Result to convert; its payload is moved out on success.
 * @return Converted receive_result.
 */
[[nodiscard]] handshake_client_base::receive_result to_receive_result(
    rdma::handshake::receive_result r);

} // namespace limestone::replication
