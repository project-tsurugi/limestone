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

// Derived class implementing the pure virtual functions of replication_message
class message_session_begin : public replication_message {
public:
    void set_param(std::string configuration_id, uint64_t epoch_number);

    void send_body(socket_io& io) const override;
    void receive_body(socket_io& io) override;
    [[nodiscard]] message_type_id get_message_type_id() const override;

    // Process the message after it has been received.
    // Empty implementation for message_session_begin
    void post_receive(socket_io& /*io*/) override {
        // No specific processing needed for message_session_begin
    }

    // Factory function for creating session_begin_message
    [[nodiscard]] static std::unique_ptr<replication_message> create();

    // Getters for private members
    [[nodiscard]] uint8_t get_connection_type() const { return connection_type_; }
    [[nodiscard]] uint64_t get_protocol_version() const { return protocol_version_; }
    [[nodiscard]] const std::string& get_configuration_id() const { return configuration_id_; }
    [[nodiscard]] uint64_t get_epoch_number() const { return epoch_number_; }

private:
    // Register SESSION_BEGIN in replication_message factory map.
    // The static initialization here is intentional. If an exception occurs,
    // the program should terminate immediately. We ignore the clang-tidy warning 
    // (cert-err58-cpp) as this behavior is desired.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(message_type_id::SESSION_BEGIN, &message_session_begin::create); 
        return true;
    }(); 

    uint8_t connection_type_ = CONNECTION_TYPE_CONTROL_CHANNEL;
    uint64_t protocol_version_ = 1;  // TODO: プロトコル共通のヘッダファイルに定数値で定義する。
    std::string configuration_id_{};
    uint64_t epoch_number_{};
};

}  // namespace limestone::replication
