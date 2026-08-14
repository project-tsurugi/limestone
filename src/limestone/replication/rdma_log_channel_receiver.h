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

#include <limestone/api/datastore.h>
#include <limestone/api/log_channel.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include <rdma/rdma_receive_event.h>

#include "rdma_log_entries_receiver.h"

namespace limestone::replication {

/**
 * @brief Replica-side receiving end of the RDMA data frames for one log channel.
 *
 * Validates the frame ordering, reassembles the frames, and applies the
 * completed LOG_ENTRY messages to the bound log_channel. Independent of the
 * TCP channel handlers: it depends on neither a TCP connection nor a
 * replication_message_io.
 */
class rdma_log_channel_receiver {
public:
    /**
     * @brief Constructs the receiving end.
     * @param datastore Datastore used to reconstruct LOG_ENTRY messages
     *        (construction of rdma_log_entries_receiver).
     * @param channel Log channel the received LOG_ENTRY messages are applied to.
     */
    rdma_log_channel_receiver(limestone::api::datastore& datastore,
        limestone::api::log_channel& channel) noexcept;

    virtual ~rdma_log_channel_receiver() = default;

    rdma_log_channel_receiver(rdma_log_channel_receiver const&) = delete;
    rdma_log_channel_receiver& operator=(rdma_log_channel_receiver const&) = delete;
    rdma_log_channel_receiver(rdma_log_channel_receiver&&) = delete;
    rdma_log_channel_receiver& operator=(rdma_log_channel_receiver&&) = delete;

    /**
     * @brief Handles an RDMA data event addressed to this log channel.
     * @param event RDMA data event to process.
     */
    virtual void handle_rdma_data_event(rdma_data_event const& event);

    /**
     * @brief Returns the bound log channel.
     */
    [[nodiscard]] limestone::api::log_channel& get_log_channel() noexcept;

protected:
    /**
     * @brief Processes the pending RDMA frames in sequence-number order.
     *
     * This assumes the caller already holds mutex_.
     */
    void process_pending_frames_locked();

    /**
     * @brief Feeds one validated frame payload to the streaming receiver.
     *
     * The payload may contain a partial LOG_ENTRY message, one complete
     * message, or multiple serialized LOG_ENTRY messages. Completed
     * message_log_entries objects are immediately applied to the log channel.
     * This assumes the caller already holds mutex_.
     *
     * @param payload Validated RDMA frame payload bytes.
     * @param last_header Header of the frame that carried the payload.
     */
    void process_payload_locked(std::vector<std::uint8_t> const& payload,
        rdma_frame_header const& last_header);

    /**
     * @brief Test helper to enqueue a pending RDMA frame directly.
     * @param event RDMA frame to enqueue.
     */
    void push_pending_frame_for_test(rdma_data_event const& event);

private:
    limestone::api::datastore& datastore_;
    limestone::api::log_channel& channel_;
    std::uint16_t next_sequence_number_{0};  ///< Expected next sequence number (wraps at 16 bits).
    std::mutex mutex_;
    std::vector<rdma_data_event> pending_frames_;
    std::unique_ptr<rdma_log_entries_receiver> entries_receiver_{};
};

} // namespace limestone::replication
