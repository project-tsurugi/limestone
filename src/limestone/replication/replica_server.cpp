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

#include <glog/logging.h>
#include <limestone/logging.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>

#include "channel_handler_base.h"
#include "control_channel_handler.h"
#include "log_channel_handler.h"
#include "logging_helper.h"
#include "message_error.h"
#include "limestone_exception_helper.h"
#include "blob_socket_io.h"
#include "datastore_impl.h"

namespace limestone::replication {

void replica_server::initialize(const boost::filesystem::path& location) {
    location_ = location;
    if (location_.empty()) {
        LOG_LP(FATAL) << "Error: Invalid location for replica server";
        throw limestone_exception(exception_type::initialization_failure, "Invalid location for replica server");
    }
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(location);
    const boost::filesystem::path& metadata_location = location;
    limestone::api::configuration conf(data_locations, metadata_location);
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
     // Avoid reinterpret_cast by using two static_casts.
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
 
 void replica_server::accept_loop() {


    while (true) {
        std::array<pollfd, 2> fds{};
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            fds[0].fd = sockfd_;
            fds[1].fd = event_fd_;
        }
        fds[0].events = POLLIN;
        fds[1].events = POLLIN;

        int ret = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), -1);
        if (ret < 0) {
            if (errno == EINTR) continue;
            LOG_LP(ERROR) << "poll() failed: " << strerror(errno);
            break;
        }

        if ((static_cast<unsigned>(fds[1].revents) & static_cast<unsigned>(POLLIN)) != 0) {
            uint64_t value = 0;
            while (true) {
                ssize_t n = ::read(event_fd_, &value, sizeof(value));
                if (n == sizeof(value)) {
                    break;
                } else if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
                    continue;
                } else {
                    LOG_LP(ERROR) << "Failed to read from eventfd in accept_loop: " << strerror(errno);
                    break;  // It is not desirable for data to remain in eventfd, but since shutdown will follow, this is acceptable.
                }
            }
            break;  // Exit the poll loop
        }

        if ((static_cast<unsigned>(fds[0].revents) & static_cast<unsigned>(POLLIN)) != 0) {
            int client_fd = -1;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                client_fd = ::accept(sockfd_, nullptr, nullptr);
            }
            if (client_fd < 0) {
                LOG_LP(ERROR) << "accept() failed: " << strerror(errno);
                break;
            }
            TRACE << "Accepted new client connection: " << client_fd;
            std::thread(&replica_server::handle_client, this, client_fd).detach();
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
        LOG_LP(FATAL) << "Warning: failed to set SO_KEEPALIVE: " << strerror(errno);
    }
    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        LOG_LP(FATAL) << "Warning: failed to set TCP_NODELAY: " << strerror(errno);
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
            handler->run(std::move(msg));
        } else {
            LOG_LP(ERROR) << "Unexpected message type: " << static_cast<uint16_t>(type);
            auto error = std::make_unique<message_error>();
            error->set_error(1, "Unexpected message type");
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

void replica_server::shutdown() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (event_fd_ >= 0) {
        uint64_t v = 1;
        while (true) {
            ssize_t n = ::write(event_fd_, &v, sizeof(v));
            // Although write failure is highly unlikely, error handling is implemented
            if (n == sizeof(v)) {
                break;  // Write succeeded
            } else if (n < 0 && errno == EINTR) {
                continue;  // Retry due to interruption
            } else {
                LOG_LP(FATAL) << "eventfd write failed in shutdown: " << strerror(errno);
                break;
            }
        }
    }
    if (sockfd_ >= 0) {
        ::shutdown(sockfd_, SHUT_RDWR);
        ::close(sockfd_);
        sockfd_ = -1;
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

bool replica_server::mark_control_channel_created() noexcept {
    bool expected = false;
    bool ret = control_channel_created_.compare_exchange_strong(expected, true);
    return ret;
}

 } // namespace limestone::replication
 