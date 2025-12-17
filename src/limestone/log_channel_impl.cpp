#include "log_channel_impl.h"
#include <chrono>
#include <string>
#include <vector>

#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"
#include "replication/replication_message.h"
#include "replication/socket_io.h"
#include "limestone/api/datastore.h"
#include "limestone/logging.h"

namespace limestone::api {

using limestone::replication::message_type_id;

log_channel_impl::log_channel_impl()
    : rdma_serializer_io_(std::string{}) {
}
log_channel_impl::~log_channel_impl() = default;

// The `message_log_entries` could be created by the caller after checking the validity of `replica_connector_`,
// but doing so would require adding an `if` statement every time, which introduces redundancy and increases the risk of bugs.
// Using a lambda allows us to encapsulate the validity check of `replica_connector_` within the function,
// preventing unnecessary message creation and avoiding redundant code in the caller. This helps keep the code concise
// and reduces the chances of errors caused by missing the `if` check.
bool log_channel_impl::send_replica_message(
        uint64_t epoch_id,
        const std::function<void(replication::message_log_entries&)>& modifier) {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);

    // If replica_connector_ is invalid, exit the function
    if (!replica_connector_) {
        return false;
    }
    // Create and modify the message
    replication::message_log_entries message{epoch_id};
    modifier(message);

    if (rdma_send_stream_) {
        // Serialize message into in-memory buffer via string-mode socket_io for RDMA path.
        rdma_serializer_io_.reset_output_buffer();
        replication::replication_message::send(rdma_serializer_io_, message);
        auto payload = rdma_serializer_io_.get_out_string();

        std::vector<std::uint8_t> bytes(payload.begin(), payload.end());
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            auto result = rdma_send_stream_->send_bytes(bytes, offset, bytes.size() - offset);
            if (! result.success) {
                LOG_LP(FATAL) << "RDMA send_bytes failed: " << result.error_message;
            }
            offset += result.bytes_written;
        }
        return true;
    }

    // Send the message
    if (!replica_connector_->send_message(message)) {
        LOG_LP(FATAL) << "Failed to send message to replica";
        replica_connector_.reset();
        return false;
    }
    return true;
}

void log_channel_impl::wait_for_replica_ack() {
    // If replica_connector_ is invalid, exit the function
    if (!replica_connector_) {
        return;
    }

    auto ack = replica_connector_->receive_message();
    auto mid = ack->get_message_type_id();
    if (mid != message_type_id::COMMON_ACK) {
        LOG_LP(FATAL) << "Protocol error: expected ACK message, but received " << static_cast<int>(mid);
        replica_connector_.reset();
    }
}

void log_channel_impl::flush_rdma_stream() {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    if (! rdma_send_stream_) {
        LOG_LP(FATAL) << "RDMA flush requested without RDMA send stream.";
    }
    auto flush_result = rdma_send_stream_->flush(std::chrono::milliseconds{5000});
    if (! flush_result.success) {
        LOG_LP(FATAL) << "RDMA flush failed: " << flush_result.error_message;
    }
}

std::future<void> log_channel_impl::flush_rdma_stream_async() {
    std::call_once(ack_thread_pool_once_, [this]() {
        ack_thread_pool_ = std::make_unique<boost::asio::thread_pool>(1);
    });
    auto promise = std::make_shared<std::promise<void>>();
    auto fut = promise->get_future();
    boost::asio::post(*ack_thread_pool_, [this, promise]() {
        try {
            this->flush_rdma_stream();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });
    return fut;
}

void log_channel_impl::set_rdma_send_stream(std::unique_ptr<rdma::communication::rdma_send_stream> stream) noexcept {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    rdma_send_stream_ = std::move(stream);
}

bool log_channel_impl::has_rdma_send_stream() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    return rdma_send_stream_ != nullptr;
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
