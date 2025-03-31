#include "log_channel_impl.h"
#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"
#include "limestone/api/datastore.h"
#include "limestone/logging.h"

namespace limestone::api {


log_channel_impl::log_channel_impl() = default;
log_channel_impl::~log_channel_impl() = default;

void log_channel_impl::send_replica_message(uint64_t epoch_id, const std::function<void(replication::message_log_entries&)>& modifier) {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    if (replica_connector_) {
        replication::message_log_entries message{epoch_id};
        modifier(message);
        if (!replica_connector_->send_message(message)) {
            LOG_LP(FATAL) << "Failed to send message to replica";
            replica_connector_.reset();
        }
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
