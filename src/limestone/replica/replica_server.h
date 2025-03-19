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

#include <boost/filesystem.hpp>
#include <limestone/api/datastore.h>
#include "replication/replication_endpoint.h"
#include "limestone_exception_helper.h"


namespace limestone::replication {

class replica_server {
public:

    // Initialize the replica server (e.g., open necessary files, set up state)
    void initialize(boost::filesystem::path location);


    // Start the listener on the provided address
    bool start_listener(const struct sockaddr_in &listen_addr);

    // Accept loop runs in its own thread
    void accept_loop();

    // Handle a single client connection
    void handle_client(int client_fd);

protected:
    std::unique_ptr<limestone::api::datastore> datastore_{};
    int sockfd_{-1};
};

}   // namespace limestone::replication