#include "rdma_log_entries_receiver.h"

#include <stdexcept>

#include "replication_message.h"

namespace limestone::replication {

rdma_log_entries_receiver::rdma_log_entries_receiver(limestone::api::datastore& datastore)
    : parser_(datastore) {}

std::size_t rdma_log_entries_receiver::consume(std::string_view bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        switch (state_) {
            case state::message_type: {
                auto const type = static_cast<message_type_id>(
                        static_cast<std::uint8_t>(bytes[offset]));
                ++offset;
                if (type != message_type_id::LOG_ENTRY) {
                    throw std::runtime_error(
                            "RDMA LOG_ENTRY receiver got unexpected message type: "
                            + std::to_string(static_cast<unsigned int>(type)));
                }
                state_ = state::log_entry_body;
                break;
            }
            case state::log_entry_body: {
                std::size_t consumed = parser_.consume(bytes.substr(offset));
                offset += consumed;
                if (!parser_.complete()) {
                    return offset;
                }
                completed_messages_.push_back(parser_.take_message());
                state_ = state::message_type;
                break;
            }
        }
    }
    return offset;
}

bool rdma_log_entries_receiver::has_message() const noexcept {
    return !completed_messages_.empty();
}

std::size_t rdma_log_entries_receiver::message_count() const noexcept {
    return completed_messages_.size();
}

bool rdma_log_entries_receiver::reading_message() const noexcept {
    return state_ != state::message_type;
}

std::unique_ptr<message_log_entries> rdma_log_entries_receiver::take_message() {
    if (completed_messages_.empty()) {
        throw std::logic_error("RDMA LOG_ENTRY receiver has no complete message");
    }
    auto message = std::move(completed_messages_.front());
    completed_messages_.pop_front();
    return message;
}

}  // namespace limestone::replication
