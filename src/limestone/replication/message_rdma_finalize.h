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
 * @brief RDMA finalize request message (leader -> replica).
 *
 * Sent by the leader after every required RDMA send stream has been acquired,
 * instructing the replica to bind its data receiver to the ACK sender via
 * finalize_channel_setup_with_sender(). The body is empty: the message acts as
 * a phase-transition signal between SETUP and TRANSFER.
 */
class message_rdma_finalize : public replication_message {
public:
    message_rdma_finalize() = default;

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(replication_message_io& io) const override;
    void receive_body(replication_message_io& io) override;

    void post_receive(handler_resources& resources) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

private:
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::RDMA_FINALIZE, &message_rdma_finalize::create);
        return true;
    }();
};

}  // namespace limestone::replication
