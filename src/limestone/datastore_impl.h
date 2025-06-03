/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <atomic>
#include <memory>
#include <limestone/api/datastore.h>
#include "replication/replica_connector.h"
#include "replication/replication_endpoint.h"

namespace limestone::api {

using namespace limestone::replication;    

// Internal implementation class for datastore (Pimpl idiom).
// This header is for internal use only.
class datastore_impl {
public:
    // Default constructor initializes the backup counter to zero.
    datastore_impl();

    // Default destructor.
    ~datastore_impl();

    // Deleted copy and move constructors and assignment operators.
    datastore_impl(const datastore_impl &) = delete;
    datastore_impl &operator=(const datastore_impl &) = delete;
    datastore_impl(datastore_impl &&) = delete;
    datastore_impl &operator=(datastore_impl &&) = delete;

    // Increments the backup counter.
    void increment_backup_counter() noexcept;

    // Decrements the backup counter.
    void decrement_backup_counter() noexcept;

    // Returns true if a backup operation is in progress.
    [[nodiscard]] bool is_backup_in_progress() const noexcept;

    // Returns true if a replica exists.
    [[nodiscard]] bool has_replica() const noexcept;

    // Disables the replica.
    void disable_replica() noexcept;

    // Method to open the control channel
    [[nodiscard]] bool open_control_channel();

    // Getter for control_channel_
    [[nodiscard]] std::shared_ptr<replica_connector> get_control_channel() const noexcept;

    // Propagates the group commit message to the replica
    void propagate_group_commit(uint64_t epoch_id);

    /**
     * @brief Checks if the replication endpoint is configured.
     * @return true if a replication endpoint is defined via the environment variable, false otherwise.
     */
    [[nodiscard]] bool is_replication_configured() const noexcept;

    [[nodiscard]] std::unique_ptr<replication::replica_connector> create_log_channel_connector(datastore &ds);

    // Getter for the datastore role (master or replica)
    [[nodiscard]] bool is_master() const noexcept;

    // Set the role to replica (switch from master to replica)
    void set_replica_role() noexcept;

    // Getter for REPLICATION_ASYNC_SESSION_CLOSE environment variable presence
    [[nodiscard]] bool is_async_session_close_enabled() const noexcept;

    // Getter for REPLICATION_ASYNC_GROUP_COMMIT environment variable presence
    [[nodiscard]] bool is_async_group_commit_enabled() const noexcept;

private:
    // Atomic counter for tracking active backup operations.
    std::atomic<int> backup_counter_;
    std::atomic<bool> replica_exists_;

    // Role flag (true = master, false = replica)
    bool is_master_ = true;

    // Private field to hold the control channel
    std::shared_ptr<replica_connector> control_channel_;

    // Replication endpoint to retrieve connection info
    replication::replication_endpoint replication_endpoint_;

    // Environment variable flags
    bool async_session_close_enabled_;
    bool async_group_commit_enabled_;
};

}  // namespace limestone::api
