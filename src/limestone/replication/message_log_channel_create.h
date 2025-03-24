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
    void set_secret(std::string secret);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(socket_io& io) const override;
    void receive_body(socket_io& io) override;
    void post_receive() override {}

    [[nodiscard]] static std::unique_ptr<replication_message> create();

    [[nodiscard]] uint8_t get_connection_type() const { return connection_type_; }
    [[nodiscard]] const std::string& get_secret() const { return secret_; }

private:
    inline static const bool registered_ = []() {
        replication_message::register_message_type(
            message_type_id::LOG_CHANNEL_CREATE,
            &message_log_channel_create::create);
        return true;
    }();

    uint8_t connection_type_ = CONNECTION_TYPE_LOG_CHANNEL;
    std::string secret_;
};

}  // namespace limestone::replication

