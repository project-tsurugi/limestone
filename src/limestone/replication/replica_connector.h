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

 #pragma once



 #include <memory>
 #include <string>
 #include <netdb.h>
 #include <cstdint>
 
#include "replication_message.h"
#include <limestone/api/datastore.h>

namespace limestone::replication {

using limestone::api::datastore;    
class replica_connector {
public:
    replica_connector() = default;
    ~replica_connector();

    // Deleted copy constructor and assignment operator to prevent copying.
    replica_connector(const replica_connector &) = delete;
    replica_connector &operator=(const replica_connector &) = delete;
    replica_connector(replica_connector &&) noexcept = default;
    replica_connector &operator=(replica_connector &&) noexcept = default;

    // Connect to the replica server at the given host and port.
    // Returns true on success, false otherwise.
    bool connect_to_server(const std::string &host, uint16_t port);

    // Connect to the replica server at the given host and port with blob support with blob support.
    // Returns true on success, false othersrc/limestone/rotation_result.hwise.
    bool connect_to_server(const std::string &host, uint16_t port, datastore &ds);

    // Send a replication message over the established TCP session.
    // Returns true on success, false otherwise.
    bool send_message(const replication_message &msg);

    // Receive a replication message from the TCP session.
    // Returns a unique_ptr to the replication_message, or nullptr on failure.
    std::unique_ptr<replication_message> receive_message();

    // Close the TCP session.
    void close_session();

private:
    [[nodiscard]] bool connect_to_server_common(const std::string &host, uint16_t port, struct ::addrinfo **res, int &socket_fd);
    std::unique_ptr<socket_io> socket_io_{};
};

}  // namespace limestone::replication
