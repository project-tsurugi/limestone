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

#include "replica_server.h"

#include <filesystem>
#include <glog/logging.h>
#include <limestone/logging.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <type_traits>
#include <variant>
#include <unistd.h>

#include <glog/logging.h>
#include <limestone/logging.h>
#include <rdma_comm/rdma_config.h>
#include <rdma_comm/unique_fd.h>

#include "channel_handler_base.h"
#include "control_channel_handler.h"
#include "log_channel_handler.h"
#include "logging_helper.h"
#include "message_error.h"
#include "limestone_exception_helper.h"
#include "blob_socket_io.h"
#include "datastore_impl.h"
#include "message_log_channel_create.h"

namespace limestone::replication {

void replica_server::initialize(const boost::filesystem::path& location) {
    location_ = location;
    if (location_.empty()) {
        LOG_LP(FATAL) << "Error: Invalid location for replica server";
        throw limestone_exception(exception_type::initialization_failure, "Invalid location for replica server");
    }
    limestone::api::configuration conf{};
    conf.set_data_location(std::filesystem::path(location.native()));
    datastore_ = std::make_unique<limestone::api::datastore>(conf);
    datastore_->get_impl()->set_replica_role();

    handler_factories_[message_type_id::SESSION_BEGIN] = [this](socket_io& io) {
        return std::make_shared<control_channel_handler>(*this, io);
    };
    
    handler_factories_[message_type_id::LOG_CHANNEL_CREATE] = [this](socket_io& io) {
        return std::make_shared<log_channel_handler>(*this, io);
    };

    // To ensure memory visibility across threads, since datastore_ and location_ might be accessed from other threads,
    // an atomic_thread_fence is used.
    atomic_thread_fence(std::memory_order_release);
}

 bool replica_server::start_listener(const struct sockaddr_in &listen_addr) {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        event_fd_ = ::eventfd(0, EFD_NONBLOCK);
        if (event_fd_ < 0) {
            LOG_LP(ERROR) << "Error: Failed to create eventfd: " << strerror(errno);
            return false;
        }
    }
    
    // Create a TCP socket and assign to sockfd_.
     sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
     if (sockfd_ < 0) {
         LOG_LP(ERROR) << "Error: Failed to create socket";
         return false;
     }
     
     // Set socket options (allow address reuse)
     int opt = 1;
     if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
         ::close(sockfd_);
         LOG_LP(ERROR) << "Error: Failed to set socket options";
         return false;
     }

     // Bind the socket to the provided address.
     // NOLINTNEXTLINE(bugprone-casting-through-void, cppcoreguidelines-pro-type-cstyle-cast)
     auto addr_ptr = static_cast<const struct sockaddr*>(static_cast<const void*>(&listen_addr));
     if (bind(sockfd_, addr_ptr, sizeof(listen_addr)) < 0) {
         ::close(sockfd_);
         LOG_LP(ERROR) << "Error: Failed to bind socket to the specified address";
         return false;
     }

     // Listen on the socket.
     if (::listen(sockfd_, SOMAXCONN) < 0) {
         ::close(sockfd_);
         LOG_LP(ERROR) << "Error: Failed to listen on socket";
         return false;
     }

     return true;
 }
 
 replica_server::poll_result replica_server::poll_shutdown_event_or_client() {
    std::array<pollfd, 2> fds{};
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        fds[0].fd = sockfd_;
        fds[1].fd = event_fd_;
    }
    fds[0].events = POLLIN;
    fds[1].events = POLLIN;

    while (true) {
        int ret = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), -1);
        if (ret < 0) {
            if (errno == EINTR) continue;
            LOG_LP(ERROR) << "poll() failed: " << strerror(errno);
            return poll_result::poll_error;
        }

        if ((static_cast<unsigned>(fds[1].revents) & static_cast<unsigned>(POLLIN)) != 0) {
            return poll_result::shutdown_event;
        }

        if ((static_cast<unsigned>(fds[0].revents) & static_cast<unsigned>(POLLIN)) != 0) {
            return poll_result::client_event;
        }
    }
}

// NOLINTNEXTLINE(readability-make-member-function-const) 
void replica_server::handle_shutdown_event() {
    uint64_t value = 0;
    while (true) {
        ssize_t n = ::read(event_fd_, &value, sizeof(value));
        if (n == sizeof(value)) {
            break;
        }
        if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        }
        if (n != sizeof(value)) {
            LOG_LP(ERROR) << "Failed to read from eventfd in accept_loop: " << strerror(errno);
            break;
        }
    }
}

void replica_server::accept_new_client() {
    int client_fd = -1;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        client_fd = ::accept(sockfd_, nullptr, nullptr);
    }
    if (client_fd < 0) {
        LOG_LP(ERROR) << "accept() failed: " << strerror(errno);
        return;
    }
    TRACE << "Accepted new client connection: " << client_fd;
    
    // Launch async task and store future
    auto future = std::async(std::launch::async, &replica_server::handle_client, this, client_fd);
    
    {
        std::lock_guard<std::mutex> lock(futures_mutex_);
        client_futures_.emplace_back(std::move(future));
    }
}

void replica_server::cleanup_completed_futures() {
    std::lock_guard<std::mutex> lock(futures_mutex_);
    for (auto it = client_futures_.begin(); it != client_futures_.end();) {
        if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            // When erasing an element, erase returns an iterator to the next element
            it = client_futures_.erase(it);
        } else {
            // If not erasing, manually advance to the next element
            ++it;
        }
    }
}

void replica_server::accept_loop() {
    while (true) {
        // Clean up completed futures without blocking
        cleanup_completed_futures();
        
        poll_result result = poll_shutdown_event_or_client();

        if (result == poll_result::poll_error) {
            break;
        }

        if (result == poll_result::shutdown_event) {
            handle_shutdown_event();
            break;
        }

        if (result == poll_result::client_event) {
            accept_new_client();
        }
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        ::close(event_fd_);
        event_fd_ = -1;
    }
}

void replica_server::register_handler(message_type_id type, std::function<std::shared_ptr<channel_handler_base>(socket_io&)> factory) noexcept {
    TRACE_START << "type: " << static_cast<uint16_t>(type);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        handler_factories_.emplace(type, std::move(factory));
    }
    TRACE_END << "handler_factories contains " << handler_factories_.size() << " factories";
}


void replica_server::clear_handlers() noexcept {
    TRACE_START;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        handler_factories_.clear();
    }
    TRACE_END << "hanlers contains " << handler_factories_.size() << " handlers";
}

void replica_server::handle_client(int client_fd) {
    TRACE_START << "client_fd: " << client_fd;
    int opt = 1;
    if (setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0) {
        LOG_LP(ERROR) << "Warning: failed to set SO_KEEPALIVE: " << strerror(errno);
    }
    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        LOG_LP(ERROR) << "Warning: failed to set TCP_NODELAY: " << strerror(errno);
    }

    blob_socket_io io(client_fd, *datastore_);
    try {
        auto msg = replication_message::receive(io);
        message_type_id type = msg->get_message_type_id();

        std::shared_ptr<channel_handler_base> handler;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            // ファクトリを取り出してインスタンスを生成（socket_ioを渡す）
            auto factory_it = handler_factories_.find(type);
            if (factory_it != handler_factories_.end()) {
                handler = factory_it->second(io);  // socket_ioを渡す
            }
        }
        if (handler) {
            if (type == message_type_id::LOG_CHANNEL_CREATE) {
                setup_log_channel_handler(*msg, handler, client_fd);
            }
            handler->run(std::move(msg));
        } else {
            LOG_LP(ERROR) << "Unexpected message type: " << static_cast<uint16_t>(type);
            auto error = std::make_unique<message_error>();
            error->set_error(message_error::error_unexpected_message_type, "Unexpected message type");
            replication_message::send(io, *error);
            io.flush();
        }
    } catch (const std::exception& ex) {
        // TODO: currently minimal error handling — just log and close connection.
        // Future improvements:
        //   - Differentiate exception types: protocol errors vs I/O errors vs internal errors.
        //   - Return COMMON_ERROR response to client for recoverable protocol errors.
        //   - Introduce a dedicated replication_exception hierarchy for richer error reporting.
        LOG_LP(ERROR) << "handle_client error: " << ex.what();
    }

    ::close(client_fd);
    TRACE_END;
}

void replica_server::setup_log_channel_handler(
        replication_message& msg,
        std::shared_ptr<channel_handler_base> const& handler,
        int client_fd) {
    // TODO: handle protocol errors below without fatal termination (return proper error).
    auto* create_msg = dynamic_cast<message_log_channel_create*>(&msg);
    if (create_msg == nullptr) {
        LOG_LP(FATAL) << "LOG_CHANNEL_CREATE message cast failed.";
    }
    auto channel_id = create_msg->get_channel_id();
    if (channel_id >= max_log_channel_slots) {
        LOG_LP(FATAL) << "Log channel id exceeds maximum slots: id=" << channel_id
                      << " max=" << max_log_channel_slots;
    }
    auto log_handler = std::dynamic_pointer_cast<log_channel_handler>(handler);
    if (! log_handler) {
        LOG_LP(FATAL) << "LOG_CHANNEL_CREATE handler is not log_channel_handler.";
    }

    // Register RDMA ACK channel so receiver can validate and reply to RDMA frames.
    if (rdma_receiver_) {
        auto reg_result = rdma_receiver_->register_channel(
            static_cast<rdma::communication::channel_id_type>(channel_id),
            rdma::communication::unique_fd{client_fd});
        if (! reg_result.success) {
            LOG_LP(FATAL) << "RDMA register_channel failed for id=" << channel_id
                          << " error=" << reg_result.error_message;
        }
    } else {
        // RDMA receiver not ready yet; store for deferred registration.
        std::lock_guard<std::mutex> lock(pending_rdma_channels_mutex_);
        pending_rdma_channels_.emplace_back(channel_id, client_fd);
    }
    {
        auto channel_idx = static_cast<std::size_t>(channel_id);
        std::lock_guard<std::mutex> lock(log_channel_slot_mutexes_.at(channel_idx));
        if (log_channel_handlers_.at(channel_idx)) {
            LOG_LP(FATAL) << "Duplicate log channel id registration: id=" << channel_id;
        }
        log_channel_handlers_.at(channel_idx) = std::move(log_handler);
    }
}

void replica_server::shutdown() {
    // Wait for all client threads to complete
    {
        std::lock_guard<std::mutex> lock(futures_mutex_);
        for (auto& future : client_futures_) {
            future.wait();
        }
        client_futures_.clear();
    }
    
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (event_fd_ >= 0) {
        uint64_t v = 1;
        while (true) {
            ssize_t n = ::write(event_fd_, &v, sizeof(v));
            // Although write failure is highly unlikely, error handling is implemented
            if (n == sizeof(v)) {
                break;  // Write succeeded
            }
            if (n < 0 && errno == EINTR) {
                continue;  // Retry due to interruption
            }
            LOG_LP(FATAL) << "eventfd write failed in shutdown: " << strerror(errno);
            break;
        }
    }
    if (sockfd_ >= 0) {
        ::shutdown(sockfd_, SHUT_RDWR);
        ::close(sockfd_);
        sockfd_ = -1;
    }

    // Release datastore resources (including manifest lock) when running in-process.
    if (datastore_) {
        datastore_->shutdown();
        datastore_.reset();
    }
}

limestone::api::datastore& replica_server::get_datastore() {
    // Ensure memory visibility across threads
    atomic_thread_fence(std::memory_order_acquire);
    return *datastore_;
}

boost::filesystem::path replica_server::get_location() const noexcept {
    // Ensure memory visibility across threads
    atomic_thread_fence(std::memory_order_acquire);
    return location_;
}

std::shared_ptr<log_channel_handler> replica_server::get_log_channel_handler(
    std::uint64_t id) const noexcept {
    if (id >= max_log_channel_slots) {
        return {};
    }
    std::lock_guard<std::mutex> lock(log_channel_slot_mutexes_.at(id));
    return log_channel_handlers_.at(id);
}

void replica_server::set_log_channel_handler_for_test(
    std::uint64_t id, std::shared_ptr<log_channel_handler> handler) {
    if (id >= max_log_channel_slots) {
        return;
    }
    std::lock_guard<std::mutex> lock(log_channel_slot_mutexes_.at(id));
    log_channel_handlers_.at(id) = std::move(handler);
}

void replica_server::on_rdma_receive(rdma::communication::rdma_receive_event const& event) {
    if (auto const* err = std::get_if<rdma::communication::rdma_receive_error_event>(&event)) {
        LOG_LP(ERROR) << "RDMA receive error: " << err->error_message;
        return;
    }
    if (auto const* data = std::get_if<rdma::communication::rdma_receive_data_event>(&event)) {
        handle_rdma_data_event(*data);
        return;
    }
}

void replica_server::handle_rdma_data_event(rdma::communication::rdma_receive_data_event const& event) {
    auto channel_id = event.header.channel_id;
    if (channel_id >= max_log_channel_slots) {
        LOG_LP(ERROR) << "RDMA channel id out of range: id=" << channel_id
                      << " max=" << max_log_channel_slots
                      << " (TODO: return protocol error instead of fatal).";
        return;
    }
    auto channel_idx = static_cast<std::size_t>(channel_id);

    std::shared_ptr<log_channel_handler> handler;
    {
        std::lock_guard<std::mutex> lock(log_channel_slot_mutexes_.at(channel_idx));
        handler = log_channel_handlers_.at(channel_idx);
    }
    if (! handler) {
        LOG_LP(ERROR) << "RDMA handler missing for channel id: " << channel_id;
        return;
    }

    handler->handle_rdma_data_event(event);
}

bool replica_server::mark_control_channel_created() noexcept {
    bool expected = false;
    bool ret = control_channel_created_.compare_exchange_strong(expected, true);
    return ret;
}

replica_server::rdma_init_result replica_server::initialize_rdma_receiver(uint32_t slot_count) {
    std::lock_guard<std::mutex> lock(rdma_init_mutex_);
    if (rdma_receiver_) {
        return rdma_init_result::already_initialized;
    }

    rdma::communication::rdma_config config{};
    auto capacity = static_cast<std::size_t>(slot_count);
    constexpr std::size_t chunk_size = 4096U;
    config.send_buffer.region_size_bytes = capacity * chunk_size;
    config.send_buffer.chunk_size_bytes = chunk_size;
    config.send_buffer.ring_capacity = capacity;
    config.remote_buffer = config.send_buffer;
    config.completion_queue_depth = 1024U;

    rdma_receiver_ = std::make_unique<rdma::communication::rdma_receiver>(config);
    auto result = rdma_receiver_->initialize(
        [this](const rdma::communication::rdma_receive_event& event) { this->on_rdma_receive(event); });
    if (! result.success) {
        rdma_receiver_.reset();
        return rdma_init_result::failed;
    }

    auto dma_address = rdma_receiver_->get_dma_address();
    if (! dma_address.has_value()) {
        rdma_receiver_.reset();
        return rdma_init_result::failed;
    }

    // Drain any pending channel registrations queued before RDMA receiver was ready.
    {
        std::lock_guard<std::mutex> lock(pending_rdma_channels_mutex_);
        for (auto& entry : pending_rdma_channels_) {
            auto fd = entry.second;
            auto reg_result = rdma_receiver_->register_channel(
                static_cast<rdma::communication::channel_id_type>(entry.first),
                rdma::communication::unique_fd{fd});
            if (! reg_result.success) {
                LOG_LP(FATAL) << "RDMA deferred register_channel failed for id=" << entry.first
                              << " error=" << reg_result.error_message;
            }
        }
        pending_rdma_channels_.clear();
    }

    return rdma_init_result::success;
}

std::optional<rdma::communication::dma_address_type> replica_server::get_rdma_dma_address() const noexcept {
    if (! rdma_receiver_) {
        return std::nullopt;
    }
    return rdma_receiver_->get_dma_address();
}

} // namespace limestone::replication
