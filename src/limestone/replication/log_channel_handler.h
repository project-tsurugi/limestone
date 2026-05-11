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

#include <limestone/api/log_channel.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

#include <rdma/rdma_receive_event.h>

#include "channel_handler_base.h"
#include "log_channel_limits.h"
#include "log_channel_handler_resources.h"
#include "rdma_log_entries_receiver.h"

namespace limestone::replication {

using limestone::api::log_channel;
class log_channel_handler : public channel_handler_base {
public:
    static constexpr int MAX_LOG_CHANNEL_COUNT =
        static_cast<int>(log_channel_slots_limit);

    /**
     * @brief Tag type to select the RDMA-only constructor.
     *
     * In RDMA-only mode the handler is registered via RDMA_FINALIZE and has no
     * per-channel TCP socket, so the channel_handler_base reference is bound to
     * an internal sentinel replication_message_io that is never used.
     */
    struct rdma_only_tag {};

    explicit log_channel_handler(replica_server &server, replication_message_io& io) noexcept;

    /**
     * @brief Construct an RDMA-only handler with no TCP socket.
     *
     * The internal sentinel io is created in the delegating private constructor
     * so that the channel_handler_base reference is valid before the body runs.
     * Callers must set the bound log_channel via bind_log_channel() before any
     * RDMA frame is delivered to this handler.
     */
    log_channel_handler(replica_server &server, rdma_only_tag tag);

    ~log_channel_handler() override = default;

    // Delete copy and move constructors and assignment operators
    log_channel_handler(const log_channel_handler &) = delete;
    log_channel_handler &operator=(const log_channel_handler &) = delete;
    log_channel_handler(log_channel_handler &&) = delete;
    log_channel_handler &operator=(log_channel_handler &&) = delete;

    /**
     * @brief Bind the log_channel that this handler delivers frames to.
     *
     * Used by RDMA-only registration path (RDMA_FINALIZE) where the
     * log_channel is created on the replica side outside validate_initial().
     *
     * @param channel log_channel created via datastore::create_channel().
     */
    void bind_log_channel(log_channel& channel) noexcept;

    /**
     * @brief Set the internal log_channel_id_counter to a specific value for testing.
     * This method is for testing purposes only.
     */
    void set_log_channel_id_counter_for_test(int value);

    /**
     * @brief Get the log channel associated with this handler.
     */
    [[nodiscard]] log_channel& get_log_channel();

    /**
     * @brief Handle RDMA data event (payload for this log channel).
     * @param event RDMA data event to process.
     */
    virtual void handle_rdma_data_event(rdma_data_event const& event);

protected:
    // Assign a log channel and set the thread name.
    validation_result authorize() override; 

    // Validate the initial message of the channel.
    validation_result validate_initial(std::unique_ptr<replication_message> request) override;
    
    // Send the initial acknowledgement message.
    void send_initial_ack() const override;
    
    // Dispatch further messages.
    void dispatch(replication_message &message, handler_resources& resources) override;

    // Get the handler resources.
    std::unique_ptr<handler_resources> create_handler_resources() override;
    
    /**
     * @brief Process pending RDMA frames while the mutex is held.
     *
     * Validated frame payloads are fed to the streaming RDMA LOG_ENTRY
     * receiver in sequence-number order.  Message completion is determined by
     * the receiver state, not by @c rdma_frame_flag_partial_payload.
     *
     * This assumes the caller already holds rdma_mutex_.
     */
    void process_pending_rdma_messages_locked();
    /**
     * @brief Feed one validated RDMA frame payload to the streaming receiver.
     *
     * The payload may contain a partial LOG_ENTRY message, one complete
     * message, or multiple serialized LOG_ENTRY messages.  Completed
     * @c message_log_entries objects are immediately applied to the log channel
     * without sending TCP ACKs.
     *
     * @param payload Validated RDMA frame payload bytes.
     * @param last_header Header of the frame that carried @p payload.
     *
     * This assumes the caller already holds rdma_mutex_.
     */
    void process_rdma_message_locked(
        std::vector<std::uint8_t> const& payload,
        rdma_frame_header const& last_header);

    /**
     * @brief Test helper to enqueue a pending RDMA frame.
     * @param event RDMA frame to enqueue.
     */
    void push_pending_frame_for_test(rdma_data_event const& event);

private:
    /**
     * @brief Delegating ctor for rdma_only mode; sets up the sentinel io
     *        before the channel_handler_base reference is bound.
     */
    log_channel_handler(replica_server& server, rdma_only_tag tag,
                        std::unique_ptr<replication_message_io> sentinel_io) noexcept;

    std::atomic<int> log_channel_id_counter{0};
    log_channel* log_channel_{nullptr};
    std::uint16_t next_sequence_number_{0};  ///< Expected next sequence number (wraps at 16 bits).
    std::mutex rdma_mutex_;
    std::vector<rdma_data_event> pending_rdma_frames_;
    std::unique_ptr<rdma_log_entries_receiver> rdma_receiver_{};

    /**
     * @brief Sentinel io kept alive for the lifetime of an rdma_only handler.
     *
     * Owned only when constructed with rdma_only_tag. The base class holds a
     * reference to *sentinel_io_; the sentinel is never read or written.
     *
     * TODO: This is a workaround for the design coupling in channel_handler_base
     * which assumes "one handler == one TCP connection". A proper fix is to
     * separate the TCP-bound handler concerns from the log_channel handler
     * concerns (e.g., introduce a log_channel_handler_base that does not depend
     * on replication_message_io, and have the TCP and RDMA variants derive from
     * it independently). Tracked in TODO.md.
     */
    std::unique_ptr<replication_message_io> sentinel_io_{};
};

} // namespace limestone::replication
