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

#include <cstdint>
#include <memory>

#include <replication/replication_message.h>

namespace limestone::replication {

/**
 * @brief RDMA initialization request message.
 */
class message_rdma_init : public replication_message {
public:
    /**
     * @brief Construct message with requested RDMA slot count.
     * @param slot_count requested RDMA slot count.
     */
    explicit message_rdma_init(uint32_t slot_count);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(socket_io& io) const override;
    void receive_body(socket_io& io) override;

    /**
     * @brief Placeholder for RDMA initialization handling on replica side.
     *
     * TODO: Implement RDMA initialization logic using handler resources.
     */
    void post_receive(handler_resources& resources) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

    [[nodiscard]] uint32_t get_slot_count() const { return slot_count_; }

private:
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::RDMA_INIT, &message_rdma_init::create);
        return true;
    }();

    uint32_t slot_count_{};
};

}  // namespace limestone::replication
