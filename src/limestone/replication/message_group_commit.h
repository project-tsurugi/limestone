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

#include <cstddef>
#include <cstdint>

#include "replication_message.h"

namespace limestone::replication {

class message_group_commit : public replication_message {
public:
    /**
     * @brief Serialized wire size in bytes.
     *
     * The fixed-length sum of the type id (uint8) written by
     * replication_message::send() and the epoch number (uint64) written by
     * send_body(). Update this value together with any serialization change.
     */
    static constexpr std::size_t wire_size = sizeof(std::uint8_t) + sizeof(std::uint64_t);

    explicit message_group_commit(uint64_t epoch_number = 0);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(replication_message_io& io) const override;
    void receive_body(replication_message_io& io) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

    [[nodiscard]] uint64_t epoch_number() const;

    void post_receive(handler_resources& resources) override;
private:
    // Register GROUP_COMMIT in replication_message factory map.
    // The static initialization here is intentional. If an exception occurs,
    // the program should terminate immediately. We ignore the clang-tidy warning 
    // (cert-err58-cpp) as this behavior is desired.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(message_type_id::GROUP_COMMIT, &message_group_commit::create);
        return true;
    }();

    uint64_t epoch_number_;
};

} // namespace limestone::replication


