#include "log_channel_impl.h"
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <string>

#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"
#include "replication/replication_message.h"
#include "replication/replication_message_io.h"
#include "rdma/rdma_replication_message_io.h"
#include "limestone/api/datastore.h"
#include "limestone/api/log_channel.h"
#include "limestone/logging.h"
#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "logging_helper.h"

namespace limestone::api {

using limestone::replication::rdma_flush_timeout;
using limestone::replication::rdma_send_stream_base;

using limestone::replication::message_type_id;

log_channel_impl::log_channel_impl()
    : rdma_serializer_io_(std::string{}) {
}
log_channel_impl::~log_channel_impl() = default;

void log_channel_impl::set_log_channel(log_channel& channel) noexcept {
    channel_ = &channel;
}

void log_channel_impl::begin_session_at(epoch_id_type epoch) {
    try {
        auto& channel = *channel_;
        channel.current_epoch_id_.store(epoch);
        TRACE_START << "current_epoch_id_=" << epoch;

        auto log_file = channel.file_path();
        channel.strm_ = fopen(log_file.c_str(), "a");  // NOLINT(*-owning-memory)
        if (!channel.strm_) {
            LOG_AND_THROW_IO_EXCEPTION("cannot make file on " + channel.location_.string(), errno);
        }
        setvbuf(channel.strm_, nullptr, _IOFBF, 128L * 1024L);  // NOLINT, NB. glibc may ignore size when _IOFBF and buffer=NULL
        channel.register_session_file(log_file);
        log_entry::begin_session(channel.strm_, epoch);
        send_replica_message(static_cast<uint64_t>(epoch), [](replication::message_log_entries &msg) {
            msg.set_session_begin_flag(true);
        });
        TRACE_END;
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel_impl::end_session_at(epoch_id_type epoch) {
    try {
        auto session_epoch = channel_->current_epoch_id_.load();
        if (session_epoch == UINT64_MAX) {
            LOG_AND_THROW_EXCEPTION("session end received while no session is open on this channel");
        }
        if (session_epoch != epoch) {
            LOG_AND_THROW_EXCEPTION("session end epoch mismatch: the message carries "
                + std::to_string(epoch) + " but the session was begun at " + std::to_string(session_epoch));
        }
        channel_->end_session();
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

std::string_view log_channel_impl::to_string_view(replica_mode mode) noexcept {
    switch (mode) {
        case replica_mode::none: return "none";
        case replica_mode::tcp: return "tcp";
        case replica_mode::rdma: return "rdma";
    }
    return "unknown";
}

std::ostream& operator<<(std::ostream& out, log_channel_impl::replica_mode mode) {
    return out << log_channel_impl::to_string_view(mode);
}

log_channel_impl::replica_mode log_channel_impl::get_replica_mode_locked() const noexcept {
    if (rdma_send_stream_) {
        return replica_mode::rdma;
    }
    if (replica_connector_) {
        return replica_mode::tcp;
    }
    return replica_mode::none;
}

log_channel_impl::replica_mode log_channel_impl::get_replica_mode() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    return get_replica_mode_locked();
}

// The `message_log_entries` could be created by the caller after checking the validity of `replica_connector_`,
// but doing so would require adding an `if` statement every time, which introduces redundancy and increases the risk of bugs.
// Using a lambda allows us to encapsulate the validity check of `replica_connector_` within the function,
// preventing unnecessary message creation and avoiding redundant code in the caller. This helps keep the code concise
// and reduces the chances of errors caused by missing the `if` check.
bool log_channel_impl::send_replica_message(
        uint64_t epoch_id,
        const std::function<void(replication::message_log_entries&)>& modifier) {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    auto mode = get_replica_mode_locked();
    TRACE_START << "epoch=" << epoch_id << " mode=" << mode;

    if (mode == replica_mode::none) {
        TRACE_ABORT << "no replica transport available";
        return false;
    }

    replication::message_log_entries message{epoch_id};
    modifier(message);

    switch (mode) {
        case replica_mode::rdma: {
            if (message.has_any_blobs()) {
                // BLOBs must be sent directly via RDMA without in-memory buffering.
                // First flush any accumulated non-blob data, then send the blob message
                // using rdma_replication_message_io which streams blob file data chunk-by-chunk.
                if (! datastore_) {
                    LOG_LP(FATAL) << "datastore not set; cannot send blob via RDMA";
                }
                flush_rdma_serializer_io_locked();
                replication::rdma_replication_message_io rdma_io(*rdma_send_stream_, *datastore_);
                replication::replication_message::send(rdma_io, message);
                // Flush any remaining non-blob serialized data left in the rdma_io buffer.
                auto remaining = rdma_io.get_out_view();
                if (! remaining.empty()) {
                    send_rdma_bytes_locked(remaining);
                    // Drop the sent bytes so the destructor's flush() does not copy
                    // them into the input stream needlessly.
                    rdma_io.reset_output_buffer();
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
        case replica_mode::tcp: {
            if (!replica_connector_->send_message(message)) {
                LOG_LP(FATAL) << "Failed to send message to replica";
                replica_connector_.reset();
                return false;
            }
            TRACE_END << "path=tcp";
            return true;
        }
        case replica_mode::none:
            // unreachable: handled by the early return above.
            break;
    }
    return false;
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
    if (get_replica_mode_locked() != replica_mode::rdma) {
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
    send_rdma_bytes_locked(rdma_serializer_io_.get_out_view());
    rdma_serializer_io_.reset_output_buffer();
}

void log_channel_impl::send_rdma_bytes_locked(std::string_view payload) {
    // No retry loop here: acquire_frame_buffer() already blocks while the send ring is full,
    // so that is where backpressure is absorbed. A failure means the ring never drained
    // within the transport's timeout, i.e. the replica is gone -- and this layer has no
    // recovery action for that. Throwing lands in log_channel's catch(...) and aborts, the
    // same outcome flush_rdma_stream() produces via LOG_LP(FATAL).
    auto result = rdma_send_stream_->send_all_bytes(payload);
    if (! result.success) {
        LOG_AND_THROW_IO_EXCEPTION(
            "failed to send bytes over RDMA: " + result.error_message, EIO);
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

void log_channel_impl::set_rdma_send_stream(std::unique_ptr<rdma_send_stream_base> stream) noexcept {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    rdma_send_stream_ = std::move(stream);
}

bool log_channel_impl::has_rdma_send_stream() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_replica_connector_);
    return get_replica_mode_locked() == replica_mode::rdma;
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
