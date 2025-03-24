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

#include "logging_helper.h"

namespace limestone::replication {

void replica_server::initialize(const boost::filesystem::path& location) {
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(location);
    const boost::filesystem::path& metadata_location = location;
    limestone::api::configuration conf(data_locations, metadata_location);
    datastore_ = std::make_unique<limestone::api::datastore>(conf);
 }
 
 bool replica_server::start_listener(const struct sockaddr_in &listen_addr) {
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
    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        event_fd_ = ::eventfd(0, EFD_NONBLOCK);
        if (event_fd_ < 0) {
            LOG_LP(ERROR) << "Error: Failed to create eventfd: " << strerror(errno);
            return;
        }
    }

    while (true) {
        std::array<pollfd, 2> fds{};
        {
            std::lock_guard<std::mutex> lock(shutdown_mutex_);
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
            {
                std::lock_guard<std::mutex> lock(shutdown_mutex_);
                ::read(event_fd_, &value, sizeof(value));
            }
            break;
        }

        if ((static_cast<unsigned>(fds[0].revents) & static_cast<unsigned>(POLLIN)) != 0) {
            int client_fd = -1;
            {
                std::lock_guard<std::mutex> lock(shutdown_mutex_);
                client_fd = ::accept(sockfd_, nullptr, nullptr);
            }
            if (client_fd < 0) {
                LOG_LP(ERROR) << "accept() failed: " << strerror(errno);
                break;
            }
            std::thread(&replica_server::handle_client, this, client_fd).detach();
        }
    }

    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        ::close(event_fd_);
        event_fd_ = -1;
    }
}

 
 void replica_server::handle_client(int client_fd) {
     // Enable TCP keep‑alive.
     int opt = 1;
     if (setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0) {
         LOG_LP(FATAL) << "Warning: failed to set SO_KEEPALIVE: " << strerror(errno) << "\n";
     }
     
     // Disable Nagle’s algorithm for lower latency.
     if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
         LOG_LP(FATAL) << "Warning: failed to set TCP_NODELAY: " << strerror(errno) << "\n";
     }
     
     ::close(client_fd);
 }
 
 void replica_server::shutdown() {
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    if (event_fd_ >= 0) {
        uint64_t v = 1;
        ::write(event_fd_, &v, sizeof(v));
    }
    if (sockfd_ >= 0) {
        ::shutdown(sockfd_, SHUT_RDWR);
        ::close(sockfd_);
        sockfd_ = -1;
    }
}
 
 } // namespace limestone::replication
 