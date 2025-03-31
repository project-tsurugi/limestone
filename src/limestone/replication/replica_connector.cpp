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



#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <sstream>
#include "replica_connector.h"
#include "blob_socket_io.h"
#include "limestone_exception_helper.h"
#include "replication_message.h"
#include "socket_io.h"

namespace limestone::replication {

replica_connector::~replica_connector() { close_session(); }

bool replica_connector::connect_to_server_common(const std::string &host, uint16_t port, struct ::addrinfo **res, int &socket_fd) {
    // Prepare address info hints
    struct ::addrinfo hints{};
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;        // IPv4
    hints.ai_socktype = SOCK_STREAM;  // TCP

    std::string port_str = std::to_string(port);
    int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, res);
    if (ret != 0) {
        LOG_LP(ERROR) << "getaddrinfo failed: " << gai_strerror(ret);
        return false;
    }

    socket_fd = ::socket((*res)->ai_family, (*res)->ai_socktype, (*res)->ai_protocol);
    if (socket_fd < 0) {
        LOG_LP(ERROR) << "Failed to create socket";
        freeaddrinfo(*res);
        return false;
    }

    if (::connect(socket_fd, (*res)->ai_addr, (*res)->ai_addrlen) < 0) {
        LOG_LP(ERROR) << "Failed to connect to server: " << strerror(errno);
        freeaddrinfo(*res);
        ::close(socket_fd);
        return false;
    }

    return true;
}

bool replica_connector::connect_to_server(const std::string &host, uint16_t port) {
    struct addrinfo *res = nullptr;
    int socket_fd = -1;
    if (!connect_to_server_common(host, port, &res, socket_fd)) {
        return false;
    }

    socket_io_ = std::make_unique<socket_io>(socket_fd);
    freeaddrinfo(res);
    return true;
}


bool replica_connector::connect_to_server(const std::string &host, uint16_t port, datastore &ds) {
    struct addrinfo *res = nullptr;
    int socket_fd = -1;
    if (!connect_to_server_common(host, port, &res, socket_fd)) {
        return false;
    }

    
    socket_io_ = std::make_unique<blob_socket_io>(socket_fd, ds);
    if (!socket_io_) {
        ::close(socket_fd);  // Ensure socket is closed if memory allocation fails
        freeaddrinfo(res);
        return false;
    }

    freeaddrinfo(res);
    return true;
}

bool replica_connector::send_message(const replication_message &msg) {
    TRACE_START << "Sending message, message_type_id: " << static_cast<int>(msg.get_message_type_id());
    // Serialize the message into a string using the replication_message::send method.
    replication_message::send(*socket_io_, msg);
    auto ret = socket_io_->flush();
    TRACE_END << "ret = " << ret;
    return ret;
}

std::unique_ptr<replication_message> replica_connector::receive_message() {
    try {
        return replication_message::receive(*socket_io_);
    } catch (const std::exception &ex) {
        LOG_LP(ERROR) << "Exception during message reception: " << ex.what();
        return nullptr;
    }
}

void replica_connector::close_session() {
    if (socket_io_) {
        socket_io_->close();
    }
}

}  // namespace limestone::replication
