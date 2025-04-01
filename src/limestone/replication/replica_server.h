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

#include <functional>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <boost/filesystem.hpp>
#include "replication_message.h"
#include <limestone/api/datastore.h>
#include "limestone_exception_helper.h"

namespace limestone::replication {

class channel_handler_base;

class replica_server {
public:
    using handler_fn = std::function<void(int, std::unique_ptr<replication_message>)>;

    /**
     * Initialize internal datastore and metadata from the given filesystem path.
     * @param location filesystem path used for storing replication data and metadata.
     */
    void initialize(const boost::filesystem::path& location);

    /**
     * Bind and listen on the specified IPv4 address for incoming replication clients.
     * @return true if listener was successfully started; false on error.
     */
    [[nodiscard]] bool start_listener(const struct sockaddr_in& listen_addr);

    /**
     * Run the accept loop in the current thread, dispatching each new client connection
     * to handle_client(). Exits immediately when shutdown() is called.
     */
    void accept_loop();

    /**
     * Process a single client connection: configure socket options, receive exactly one
     * replication_message, invoke the registered handler (or return an error), then close.
     */
    void handle_client(int client_fd);

    /**
     * Register a handler function for a specific message_type_id.
     */
    void register_handler(message_type_id type, std::shared_ptr<channel_handler_base> handler) noexcept;

    /**
     * Clear all registered handlers. Intended for testing only.
     */
    void clear_handlers() noexcept;

    /**
     * Signal the accept_loop() to exit and close the listening socket.
     */
    void shutdown();

    /**
     * Get the underlying datastore instance.
     * @return the underlying datastore instance.
     */
    [[nodiscard]] limestone::api::datastore& get_datastore();

    /**
     * Get the location of the datastore.
     * @return the filesystem path for the datastore.
     */
    [[nodiscard]] boost::filesystem::path get_location() const noexcept;

private:
    boost::filesystem::path location_;                                                     ///< filesystem path for datastore
    std::unordered_map<message_type_id, std::shared_ptr<channel_handler_base>> handlers_;  ///< message dispatch table
    std::unique_ptr<limestone::api::datastore> datastore_;                                 ///< underlying datastore instance
    std::mutex state_mutex_;                                                               ///< mutex for thread safety
    int event_fd_{-1};                                                                     ///< eventfd used to unblock poll()
    int sockfd_{-1};                                                                       ///< listening socket file descriptor
};

} // namespace limestone::replication
