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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "replication_message.h"

namespace limestone::replication {

/**
 * @brief The compaction boundary message.
 *
 * Each time a compaction completes, the master sends the boundary epoch of the compaction and the BLOB ID it used
 * as the boundary of the BLOB file GC to the replica over the control channel. The replica sends no response.
 */
class message_compaction_boundary : public replication_message {
public:
    /**
     * @brief The serialized size in bytes.
     *
     * It is the sum of the type id written by replication_message::send() (uint8), and the boundary epoch (uint64)
     * and the BLOB ID boundary (uint64) written by send_body(). Change this value when the serialized form changes.
     */
    static constexpr std::size_t wire_size = sizeof(std::uint8_t) + sizeof(std::uint64_t) + sizeof(std::uint64_t);

    /**
     * @brief Constructs an empty message to receive into.
     */
    message_compaction_boundary() = default;

    /**
     * @brief Constructs a message to send.
     * @param boundary_epoch the boundary epoch of the compaction
     * @param blob_id_boundary the BLOB ID used as the boundary of the BLOB file GC
     */
    message_compaction_boundary(std::uint64_t boundary_epoch, std::uint64_t blob_id_boundary);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(replication_message_io& io) const override;
    void receive_body(replication_message_io& io) override;

    /**
     * @brief Creates an empty message. Registered in the creation table of replication_message.
     * @return the message
     */
    [[nodiscard]] static std::unique_ptr<replication_message> create();

    /**
     * @brief Returns the boundary epoch of the compaction.
     * @return the boundary epoch
     */
    [[nodiscard]] std::uint64_t boundary_epoch() const;

    /**
     * @brief Returns the BLOB ID used as the boundary of the BLOB file GC.
     * @return the BLOB ID boundary
     */
    [[nodiscard]] std::uint64_t blob_id_boundary() const;

    /**
     * @brief Makes the datastore of the replica keep the received boundary. No response is sent.
     * @param resources the resources of the control channel handler (control_channel_handler_resources)
     */
    void post_receive(handler_resources& resources) override;

private:
    // Register COMPACTION_BOUNDARY in the creation table of replication_message. The static initialization is
    // intentional. If an exception occurs, the program should terminate immediately, so the clang-tidy warning
    // (cert-err58-cpp) is ignored.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(message_type_id::COMPACTION_BOUNDARY,
                                                   &message_compaction_boundary::create);
        return true;
    }();

    std::uint64_t boundary_epoch_{0};
    std::uint64_t blob_id_boundary_{0};
};

} // namespace limestone::replication
