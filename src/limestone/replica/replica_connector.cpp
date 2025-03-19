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

 #include "replica_connector.h"

 #include <sys/types.h>
 #include <sys/socket.h>
 #include <netdb.h>
 #include <unistd.h>
 #include <cstring>
 #include <sstream>
 #include "network_io.h"
 #include "limestone_exception_helper.h"
 #include "replication_message.h"
 
 namespace limestone::replication {
 
 replica_connector::~replica_connector() {
     if (socket_fd_ != -1) {
         close_session();
     }
 }
 
 bool replica_connector::connect_to_server(const std::string &host, uint16_t port) {
     // Prepare address info hints
     struct addrinfo hints, *res = nullptr;
     std::memset(&hints, 0, sizeof(hints));
     hints.ai_family = AF_INET;       // IPv4
     hints.ai_socktype = SOCK_STREAM; // TCP
 
     std::string port_str = std::to_string(port);
     int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
     if (ret != 0) {
         LOG_LP(ERROR) << "getaddrinfo failed: " << gai_strerror(ret);
         return false;
     }
 
     socket_fd_ = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
     if (socket_fd_ < 0) {
         LOG_LP(ERROR) << "Failed to create socket";
         freeaddrinfo(res);
         return false;
     }
 
     if (::connect(socket_fd_, res->ai_addr, res->ai_addrlen) < 0) {
         LOG_LP(ERROR) << "Failed to connect to server: " << strerror(errno);
         freeaddrinfo(res);
         ::close(socket_fd_);
         socket_fd_ = -1;
         return false;
     }
 
     freeaddrinfo(res);
     return true;
 }
 
 bool replica_connector::send_message(const replication_message &msg) {
     // Serialize the message into a string using the replication_message::send method.
     std::ostringstream oss;
     replication_message::send(oss, msg);
     std::string data = oss.str();
 
     ssize_t bytes_sent = ::send(socket_fd_, data.data(), data.size(), 0);
     if (bytes_sent < 0 || static_cast<size_t>(bytes_sent) != data.size()) {
         LOG_LP(ERROR) << "Failed to send complete message: " << strerror(errno);
         return false;
     }
     return true;
 }
 
 std::unique_ptr<replication_message> replica_connector::receive_message() {
     // For simplicity, use a fixed buffer size.
     // Note: In a production implementation, proper message framing should be used.
     constexpr size_t buffer_size = 4096;
     char buffer[buffer_size];
     ssize_t bytes_received = ::recv(socket_fd_, buffer, buffer_size, 0);
     if (bytes_received <= 0) {
         LOG_LP(ERROR) << "Failed to receive message: " << strerror(errno);
         return nullptr;
     }
 
     std::string data(buffer, bytes_received);
     std::istringstream iss(data);
     try {
         auto msg = replication_message::receive(iss);
         return msg;
     } catch (const std::exception &ex) {
         LOG_LP(ERROR) << "Exception during message reception: " << ex.what();
         return nullptr;
     }
 }
 
 void replica_connector::close_session() {
     if (socket_fd_ != -1) {
         ::close(socket_fd_);
         socket_fd_ = -1;
     }
 }
 
 } // namespace limestone::replication
 