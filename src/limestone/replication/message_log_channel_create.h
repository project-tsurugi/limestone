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

#include "replication_message.h"

namespace limestone::replication {

class message_log_channel_create : public replication_message {
public:
    explicit message_log_channel_create(std::uint64_t channel_id) noexcept
        : channel_id_(channel_id) {}

    void set_secret(std::string secret);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(socket_io& io) const override;
    void receive_body(socket_io& io) override;

    [[nodiscard]] uint8_t get_connection_type() const { return connection_type_; }
    [[nodiscard]] const std::string& get_secret() const { return secret_; }
    [[nodiscard]] std::uint64_t get_channel_id() const noexcept { return channel_id_; }

    /**
     * @brief Factory for replication_message::receive().
     * @note channel_id is initialized as 0 for deserialization.
     */
    [[nodiscard]] static std::unique_ptr<replication_message> create_placeholder();

private:
    // Register LOG_CHANNEL_CREATE in replication_message factory map.
    // The static initialization here is intentional. If an exception occurs,
    // the program should terminate immediately. We ignore the clang-tidy warning 
    // (cert-err58-cpp) as this behavior is desired.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::LOG_CHANNEL_CREATE,
            &message_log_channel_create::create_placeholder);
        return true;
    }();

    uint8_t connection_type_ = CONNECTION_TYPE_LOG_CHANNEL;
    std::uint64_t channel_id_ = 0;
    std::string secret_;
};

}  // namespace limestone::replication
