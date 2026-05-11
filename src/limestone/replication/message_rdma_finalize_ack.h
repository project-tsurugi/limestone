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

#pragma once

#include <memory>

#include <replication/replication_message.h>

namespace limestone::replication {

/**
 * @brief RDMA finalize acknowledgement (replica -> leader).
 *
 * Sent by the replica after data_receiver finishes binding to the ack_sender
 * via finalize_channel_setup_with_sender(). Carries no body; receipt by the
 * leader signals that it is safe to call sender finalize_channel_setup() and
 * begin the TRANSFER phase.
 */
class message_rdma_finalize_ack : public replication_message {
public:
    message_rdma_finalize_ack() = default;

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(replication_message_io& io) const override;
    void receive_body(replication_message_io& io) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

private:
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::RDMA_FINALIZE_ACK, &message_rdma_finalize_ack::create);
        return true;
    }();
};

}  // namespace limestone::replication
