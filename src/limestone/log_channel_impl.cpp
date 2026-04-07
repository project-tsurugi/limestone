#include "log_channel_impl.h"
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"
#include "replication/replication_message.h"
#include "replication/socket_io.h"
#include "replication/rdma_socket_io.h"
#include "limestone/api/datastore.h"
#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::api {

namespace {

constexpr auto rdma_flush_timeout = std::chrono::milliseconds{30000};

} // namespace

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
    TRACE_START << "epoch=" << epoch_id;

    // If replica_connector_ is invalid, exit the function
    if (!replica_connector_) {
        TRACE_ABORT << "replica_connector_ is null";
        return false;
    }
    // Create and modify the message
    replication::message_log_entries message{epoch_id};
    modifier(message);

    if (rdma_send_stream_) {
        if (message.has_any_blobs()) {
            // BLOBs must be sent directly via RDMA without in-memory buffering.
            // First flush any accumulated non-blob data, then send the blob message
            // using rdma_socket_io which streams blob file data chunk-by-chunk.
            if (! datastore_) {
                LOG_LP(FATAL) << "datastore not set; cannot send blob via RDMA";
            }
            flush_rdma_serializer_io_locked();
            replication::rdma_socket_io rdma_io(*rdma_send_stream_, *datastore_);
            replication::replication_message::send(rdma_io, message);
            // Flush any remaining non-blob serialized data left in the rdma_io buffer.
            auto remaining = rdma_io.get_out_string();
            if (! remaining.empty()) {
                send_rdma_bytes_locked(remaining);
            }
            TRACE_END << "path=rdma blob";
        } else {
            // Accumulate non-blob messages in rdma_serializer_io_ and flush only when
            // the buffer reaches rdma_send_buffer_threshold (batching optimization).
            replication::replication_message::send(rdma_serializer_io_, message);
            std::size_t buffered = rdma_serializer_io_.get_out_size();
            TRACE << "RDMA path buffered_size=" << buffered;
            if (buffered >= rdma_send_buffer_threshold) {
                flush_rdma_serializer_io_locked();
                TRACE_END << "path=rdma flushed";
            } else {
                TRACE_END << "path=rdma buffered";
            }
        }
        return true;
    }

    TRACE << "TCP path";
    // Send the message
    if (!replica_connector_->send_message(message)) {
        LOG_LP(FATAL) << "Failed to send message to replica";
        replica_connector_.reset();
        return false;
    }
    TRACE_END << "path=tcp";
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
    // Drain any data remaining in the serialization buffer before issuing the RDMA flush.
    flush_rdma_serializer_io_locked();
    auto flush_result = rdma_send_stream_->flush(rdma_flush_timeout);
    if (! flush_result.success) {
        LOG_LP(FATAL) << "RDMA flush failed: " << flush_result.error_message;
    }
}

void log_channel_impl::flush_rdma_serializer_io_locked() {
    std::size_t buffered = rdma_serializer_io_.get_out_size();
    if (buffered == 0) {
        return;
    }
    auto payload = rdma_serializer_io_.get_out_string();
    send_rdma_bytes_locked(payload);
    rdma_serializer_io_.reset_output_buffer();
}

void log_channel_impl::send_rdma_bytes_locked(std::string const& payload) {
    std::vector<std::uint8_t> bytes(payload.begin(), payload.end());
    std::size_t offset = 0;
    std::size_t consecutive_failures = 0;
    while (offset < bytes.size()) {
        auto result = rdma_send_stream_->send_bytes(bytes, offset, bytes.size() - offset);
        if (! result.success) {
            ++consecutive_failures;
            LOG_LP(WARNING) << "RDMA send_bytes failed (consecutive failures="
                            << consecutive_failures << "): " << result.error_message;
            std::this_thread::sleep_for(std::chrono::seconds{1});
        } else {
            consecutive_failures = 0;
        }
        offset += result.bytes_written;
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

void log_channel_impl::set_datastore(datastore& ds) noexcept {
    datastore_ = &ds;
}

}  // namespace limestone::api
