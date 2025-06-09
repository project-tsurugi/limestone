#include "log_channel_impl.h"

#include "limestone/api/datastore.h"
#include "limestone/logging.h"
#include "now_nsec.h"
#include "replication/message_log_entries.h"
#include "replication/replica_connector.h"

namespace limestone::api {

using limestone::replication::message_type_id;

log_channel_impl::log_channel_impl() = default;
log_channel_impl::~log_channel_impl() = default;

// The `message_log_entries` could be created by the caller after checking the validity of `replica_connector_`,
// but doing so would require adding an `if` statement every time, which introduces redundancy and increases the risk of bugs.
// Using a lambda allows us to encapsulate the validity check of `replica_connector_` within the function,
// preventing unnecessary message creation and avoiding redundant code in the caller. This helps keep the code concise
// and reduces the chances of errors caused by missing the `if` check.
void log_channel_impl::send_replica_message(uint64_t epoch_id, const std::function<void(replication::message_log_entries&)>& modifier) {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    
    // If replica_connector_ is invalid, exit the function
    if (!replica_connector_) {
        return; 
    }

    uint64_t start = limestone::internal::now_nsec();

    // Create and modify the message
    replication::message_log_entries message{epoch_id};
    modifier(message);
    
    // Send the message
    if (!replica_connector_->send_message(message)) {
        LOG_LP(FATAL) << "Failed to send message to replica";
        replica_connector_.reset();
        return;
    }

    // If session_end_flag or flush_flag is set, wait for receive ACK
    if (message.has_session_end_flag() || message.has_flush_flag()) {
        auto ack = replica_connector_->receive_message();
        auto mid = ack->get_message_type_id();
        if (mid != message_type_id::COMMON_ACK) {
            LOG_LP(FATAL) << "Protocol error: expected ACK message, but received " << static_cast<int>(mid);
            replica_connector_.reset();
        }
        uint64_t end = limestone::internal::now_nsec();
        LOG_LP(INFO) << "log_channel_impl::send_replica_message() took " << (end - start) / 1000 << "us";
    }
}

void log_channel_impl::set_replica_connector(std::unique_ptr<replication::replica_connector> connector) {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    replica_connector_ = std::move(connector);
}

void log_channel_impl::disable_replica_connector() {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    replica_connector_.reset();
}

replication::replica_connector* log_channel_impl::get_replica_connector() {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    return replica_connector_.get();
}

}  // namespace limestone::api
