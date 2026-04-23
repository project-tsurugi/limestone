#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <string_view>

#include "limestone/api/datastore.h"
#include "message_log_entries.h"
#include "rdma_log_entries_parser.h"

namespace limestone::replication {

/**
 * @brief Incremental receiver for RDMA LOG_ENTRY messages.
 *
 * This class consumes serialized replication messages from RDMA payload bytes.
 * It validates the one-byte message type, then delegates the LOG_ENTRY body to
 * @c rdma_log_entries_parser.  Completed @c message_log_entries objects are
 * queued and can be taken by the caller without routing the bytes through
 * @c replication_message::receive() or @c message_log_entries::receive_body().
 *
 * A single @c consume() call may contain a partial message, one complete
 * message, or multiple complete messages followed by a partial next message.
 */
class rdma_log_entries_receiver {
public:
    explicit rdma_log_entries_receiver(limestone::api::datastore& datastore);

    rdma_log_entries_receiver(const rdma_log_entries_receiver&) = delete;
    rdma_log_entries_receiver& operator=(const rdma_log_entries_receiver&) = delete;
    rdma_log_entries_receiver(rdma_log_entries_receiver&&) = delete;
    rdma_log_entries_receiver& operator=(rdma_log_entries_receiver&&) = delete;

    ~rdma_log_entries_receiver() = default;

    /**
     * @brief Consume as many bytes as possible from @p bytes.
     *
     * @return Number of bytes consumed from @p bytes.
     *
     * @throws std::runtime_error if a message type other than LOG_ENTRY is seen.
     */
    [[nodiscard]] std::size_t consume(std::string_view bytes);

    /**
     * @brief Return true if at least one complete message is ready.
     */
    [[nodiscard]] bool has_message() const noexcept;

    /**
     * @brief Number of complete messages waiting to be taken.
     */
    [[nodiscard]] std::size_t message_count() const noexcept;

    /**
     * @brief Return true when the receiver is in the middle of a message.
     */
    [[nodiscard]] bool reading_message() const noexcept;

    /**
     * @brief Take the oldest complete message.
     *
     * @throws std::logic_error if no complete message is available.
     */
    [[nodiscard]] std::unique_ptr<message_log_entries> take_message();

private:
    enum class state {
        message_type,
        log_entry_body,
    };

    state state_{state::message_type};
    rdma_log_entries_parser parser_;
    std::deque<std::unique_ptr<message_log_entries>> completed_messages_{};
};

}  // namespace limestone::replication
