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
#include <vector>
#include <string_view>
#include <boost/filesystem.hpp>

#include "limestone/api/blob_id_type.h"
#include "limestone/api/storage_id_type.h"
#include "limestone/api/write_version_type.h"
#include "limestone/status.h"
#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"

namespace limestone::api {

class datastore;

class log_channel_impl {
public:
    log_channel_impl();
    ~log_channel_impl();

    log_channel_impl(const log_channel_impl&) = delete;
    log_channel_impl& operator=(const log_channel_impl&) = delete;
    log_channel_impl(log_channel_impl&&) = delete;
    log_channel_impl& operator=(log_channel_impl&&) = delete;
    
    /**
     * @brief Sets the replica connector instance. Ownership is transferred.
     * @param connector A unique_ptr to a replica_connector instance.
     * @note This method is for internal use only and is not part of the public API.
     *       Do not use this method in production code.
     */
    void set_replica_connector(std::unique_ptr<replication::replica_connector> connector);

    /**
     * @brief Disables the current replica connector by resetting the unique_ptr.
     * @note This method is for internal use only and is not part of the public API.
     *       Do not use this method in production code.
     */
    void disable_replica_connector();

    /**
     * @brief Test-only getter for the replica_connector instance.
     * @return A raw pointer to the replica_connector instance.
     */
    replication::replica_connector* get_replica_connector();

    /**
     * Sends a message to replica, modifying the message using the provided modifier.
     *
     * This method creates a `message_log_entries` object and applies the provided
     * `modifier` lambda to modify the message before sending it. The modifier lambda allows
     * the caller to set flags, add entries, or make other modifications to the message content.
     *
     * The function handles the creation of the message, while the lambda is responsible for
     * modifying the content of the message (e.g., setting flags or adding entries). If the system
     * is in a state where message sending is possible, the message is transmitted to the replica.
     * If the message includes session end or flush flags, the function will wait for an acknowledgment (ACK) response.
     *
     * @param epoch_id The epoch identifier to be associated with the message.
     * @param modifier A lambda function that modifies the `message_log_entries`. The lambda should take a reference
     *                 to the `message_log_entries` object and apply any required changes (e.g., setting flags, adding entries).
     */
    void send_replica_message(uint64_t epoch_id, const std::function<void(replication::message_log_entries&)>& modifier);

    /**
     * @brief Blocks the calling thread until an acknowledgment is received from the replica.
     *
     * @note This function may block indefinitely if the replica does not respond.
     *       Consider using a timeout mechanism if appropriate.
     */
    void wait_for_replica_ack();
private:
    std::unique_ptr<replication::replica_connector> replica_connector_;
    std::mutex mtx_replica_connector_;
};

}  // namespace limestone::api
