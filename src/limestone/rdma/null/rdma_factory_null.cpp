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
#include <rdma/null_rdma_receiver.h>
#include <rdma/null_rdma_sender.h>

namespace limestone::replication {

std::unique_ptr<rdma_sender_base> make_rdma_sender(std::uint32_t /*slot_count*/) {
    return std::make_unique<null_rdma_sender>();
}

std::unique_ptr<rdma_receiver_base> make_rdma_receiver(std::uint32_t /*slot_count*/) {
    return std::make_unique<null_rdma_receiver>();
}

} // namespace limestone::replication
