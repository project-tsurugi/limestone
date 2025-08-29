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

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include "backup_object.h"
#include "limestone/api/epoch_id_type.h"

namespace limestone::grpc::backend {

using limestone::api::epoch_id_type;
class session {
public:
    /**
     * @brief Type alias for the on-remove callback function.
     */
    using on_remove_callback_type = std::function<void()>;


    /**
     * @brief Constructs a new session object.
     *
     * @param session_id        The unique identifier for the session.
     * @param begin_epoch       Epoch number to start backup (inclusive). 0 means full backup.
     * @param end_epoch         Epoch number to end backup (exclusive). 0 means there is no data to back up.
     * @param expire_at         The expiration timestamp (in seconds since epoch) for the session.
     * @param on_remove         Optional callback to be invoked when the session is removed. Defaults to nullptr.
     */
    explicit session(std::string session_id, epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t expire_at, on_remove_callback_type on_remove = nullptr);

    /**
     * @brief Constructs a new session object with the specified parameters.
     *
     * @param begin_epoch        Epoch number to start backup (inclusive). 0 means full backup.
     * @param end_epoch          Epoch number to end backup (exclusive). 0 means there is no data to back up.
     * @param timeout_seconds    Timeout duration for the session, in seconds.
     * @param on_remove          Optional callback function to be invoked when the session is removed.
     */
    explicit session(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, on_remove_callback_type on_remove = nullptr);

    session(const session& other);
    session& operator=(const session& other) = delete;
    session(session&&) = delete;
    session& operator=(session&&) = delete;
    ~session() = default; 

    /**
     * @brief Refresh the session expiration.
     * @param timeout_seconds New timeout in seconds from now.
     */
    void refresh(int64_t timeout_seconds);

    /**
     * @brief Get the expiration time (UNIX timestamp).
     * @return Expiration time.
     */
    [[nodiscard]] int64_t expire_at() const;

    /**
     * @brief Call the on-remove callback if set.
     */
    void call_on_remove();

    /**
     * @brief Retrieves the unique identifier for the session.
     * 
     * @return A constant reference to the session's unique identifier string.
     */
    [[nodiscard]] const std::string& session_id() const;

    /**
     * @brief Returns the epoch number to start backup (inclusive).
     * 
     * @return The epoch number to start backup (inclusive). 0 means full backup
     */
    [[nodiscard]] epoch_id_type begin_epoch() const;

    /**
     * @brief Returns the epoch number to end backup (exclusive).
     * 
     * @return The epoch number to end backup (exclusive). 0 means there is no data to back up.
     */
    [[nodiscard]] epoch_id_type end_epoch() const;

    /**
     * @brief Add a backup_object to the session. Throws if object_id already exists.
     * @param obj backup_object to add
     */
    void add_backup_object(const backup_object& obj);

    /**
     * @brief Find a backup_object by object_id.
     * @param object_id object id string
     * @return optional backup_object (copy), or nullopt if not found
     */
    std::optional<backup_object> find_backup_object(const std::string& object_id) const;

    /**
     * @brief Get const begin iterator for backup_objects.
     */
    auto begin() const noexcept -> std::map<std::string, backup_object>::const_iterator;

    /**
     * @brief Get const end iterator for backup_objects.
     */
    auto end() const noexcept -> std::map<std::string, backup_object>::const_iterator;

private:
    mutable std::mutex backup_objects_mutex_;
    std::string generate_uuid();
    std::string session_id_;
    epoch_id_type begin_epoch_;
    epoch_id_type end_epoch_;
    std::atomic<int64_t> expire_at_;
    on_remove_callback_type on_remove_;

    // Map from object_id to backup_object
    std::map<std::string, backup_object> backup_objects_;
};

} // namespace limestone::grpc::backend
