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
 #include <cstdint>
 #include "replication_message.h"
 
 namespace limestone::replication {
 
 class replica_connector {
 public:
     replica_connector() = default;
     ~replica_connector();
 
     // Connect to the replica server at the given host and port.
     // Returns true on success, false otherwise.
     bool connect_to_server(const std::string &host, uint16_t port);
 
     // Send a replication message over the established TCP session.
     // Returns true on success, false otherwise.
     bool send_message(const replication_message &msg);
 
     // Receive a replication message from the TCP session.
     // Returns a unique_ptr to the replication_message, or nullptr on failure.
     std::unique_ptr<replication_message> receive_message();
 
     // Close the TCP session.
     void close_session();
 
 private:
     int socket_fd_ = -1;
 };
 
 } // namespace limestone::replication
 