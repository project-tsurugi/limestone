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

#include <limestone/api/datastore.h>
#include <limestone/api/blob_pool.h>

#include <atomic>
#include <array>
#include <memory>
#include <optional>
#include <functional>
#include <string>
#include <sys/types.h>

#include "manifest.h"
#include "replication/replica_connector.h"
#include "replication/replication_endpoint.h"

namespace limestone::api {

using limestone::internal::manifest;
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

    /**
     * @brief Sends a group commit message to the replica.
     * @param epoch_id The epoch ID to send.
     * @return Returns true if the message was actually sent; false otherwise.
     *         (If true, you must call wait_for_propagated_group_commit_ack().)
     */
    [[nodiscard]] bool propagate_group_commit(uint64_t epoch_id);

    // Waits for the acknowledgment of the propagated group commit message
    void wait_for_propagated_group_commit_ack();

    /**
     * @brief Checks if the replication endpoint is configured.
     * @return true if a replication endpoint is defined via the environment variable, false otherwise.
     */
    [[nodiscard]] bool is_replication_configured() const noexcept;

    /**
     * @brief Checks if the TP monitor endpoint is configured and valid.
     * @return true if TP monitor is enabled, false otherwise.
     */
    [[nodiscard]] bool is_tp_monitor_enabled() const noexcept;

    /**
     * @brief Returns TP monitor host.
     * @return Host string; empty if TP monitor is disabled.
     */
    [[nodiscard]] const std::string& tp_monitor_host() const noexcept;

    /**
     * @brief Returns TP monitor port.
     * @return Port number; 0 if TP monitor is disabled.
     */
    [[nodiscard]] int tp_monitor_port() const noexcept;

    [[nodiscard]] std::unique_ptr<replication::replica_connector> create_log_channel_connector(datastore &ds);

    // Getter for the datastore role (master or replica)
    [[nodiscard]] bool is_master() const noexcept;

    // Set the role to replica (switch from master to replica)
    void set_replica_role() noexcept;

    // Getter for REPLICATION_ASYNC_SESSION_CLOSE environment variable presence
    [[nodiscard]] bool is_async_session_close_enabled() const noexcept;

    // Getter for REPLICATION_ASYNC_GROUP_COMMIT environment variable presence
    [[nodiscard]] bool is_async_group_commit_enabled() const noexcept;

    // Getter for migration_info_
    [[nodiscard]] const std::optional<manifest::migration_info>& get_migration_info() const noexcept;

    // Setter for migration_info_
    void set_migration_info(const manifest::migration_info& info) noexcept;

    /**
     * @brief gets the HMAC secret key for BLOB reference tag generation.
     * @return reference to the HMAC secret key.
     */
    [[nodiscard]] const std::array<std::uint8_t, 16>& get_hmac_secret_key() const noexcept;

    /**
     * @brief generates a BLOB reference tag for access control.
     * @param blob_id the BLOB reference.
     * @param transaction_id the transaction ID.
     * @return the generated BLOB reference tag.
     */
    [[nodiscard]] blob_reference_tag_type generate_reference_tag(
            blob_id_type blob_id,
            std::uint64_t transaction_id) const;

    // Setter/getter for instance_id
    /**
     * @brief Sets the instance ID for this datastore.
     * @param instance_id The instance ID to store.
     */
    void set_instance_id(std::string_view instance_id);
    /**
     * @brief Returns the instance ID for this datastore.
     * @return The stored instance ID.
     */
    [[nodiscard]] const std::string& instance_id() const noexcept;

    // Setter/getter for db_name
    /**
     * @brief Sets the database name for this datastore.
     * @param db_name The database name to store.
     */
    void set_db_name(std::string_view db_name);
    /**
     * @brief Returns the database name for this datastore.
     * @return The stored database name.
     */
    [[nodiscard]] const std::string& db_name() const noexcept;

    // Setter/getter for pid
    /**
     * @brief Sets the process ID for this datastore.
     * @param pid The process ID to store.
     */
    void set_pid(pid_t pid) noexcept;
    /**
     * @brief Returns the process ID for this datastore.
     * @return The stored process ID.
     */
    [[nodiscard]] pid_t pid() const noexcept;

    /**
     * @brief Sets a custom group commit sender for tests.
     * @param sender The sender function(epoch_id) used to simulate group commit sending.
     *               The function must return true on success and false on failure.
     */
    void set_group_commit_sender_for_tests(std::function<bool(uint64_t)> const& sender);

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

    // TP monitor connection info (configured via TP_MONITOR_ENDPOINT)
    bool tp_monitor_enabled_{false};
    std::string tp_monitor_host_{};
    int tp_monitor_port_{0};

    // Environment variable flags
    bool async_session_close_enabled_;
    bool async_group_commit_enabled_;
  
    // Migration info for the manifest
    std::optional<manifest::migration_info> migration_info_;

    // HMAC secret key for BLOB reference tag generation (16 bytes)
    std::array<std::uint8_t, 16> hmac_secret_key_{};

    std::string instance_id_{"instance_id_not_set"};
    std::string db_name_{"db_name_not_set"};
    pid_t pid_{0};
    std::function<bool(uint64_t)> group_commit_sender_for_tests_{};

    /**
     * @brief generates HMAC secret key for BLOB reference tag generation.
     */
    void generate_hmac_secret_key();
};

}  // namespace limestone::api
