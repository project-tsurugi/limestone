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

#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "session.h"

namespace limestone::grpc::backend {

class session_store {
public:
    /**
     * @brief Construct a session_store and start the expiry thread.
     */
    session_store();
    
    /**
     * @brief Destructor. Stops the expiry thread and cleans up resources.
     */
    ~session_store();

    session_store(const session_store&) = delete;
    session_store& operator=(const session_store&) = delete;
    session_store(session_store&&) = delete;
    session_store& operator=(session_store&&) = delete;

    /**
     * @brief Remove a session by session_id.
     * @param session_id Session ID string.
     * @return true if erased, false if not found.
     */
    bool remove_session(const std::string& session_id);

    /**
     * @brief Retrieves a session by its ID and refreshes its expiration timeout.
     *
     * Attempts to find the session associated with the given session ID. If found,
     * the session's expiration is extended by the specified timeout in seconds.
     *
     * @param session_id The unique identifier of the session to retrieve and refresh.
     * @param timeout_seconds The number of seconds to extend the session's expiration.
     * @return std::optional<session> The session object if found; std::nullopt otherwise.
     */
    std::optional<session> get_and_refresh(const std::string& session_id, int64_t timeout_seconds);

    /**
     * @brief Creates a new session and registers it with the session store.
     *
     * This function creates a new session with the specified timeout and registers it.
     * An optional callback can be provided, which will be invoked when the session is removed.
     *
     * @param begin_epoch Epoch number to start backup (inclusive). 0 means full backup.
     * @param end_epoch Epoch number to end backup (exclusive). 0 means there is no data to back up.
     * @param timeout_seconds The timeout duration for the session, in seconds.
     * @param on_remove Optional callback function to be called when the session is removed. Defaults to nullptr.
     * @return std::optional<session> The created session if successful, or std::nullopt on failure.
     */
    std::optional<session> create_and_register(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, session::on_remove_callback_type on_remove = nullptr);

    /**
     * @brief Remove all expired sessions from the store (caller must hold lock).
     */
    void remove_expired_sessions_locked();

    /**
     * @brief Expiry thread main loop.
     */
    void session_expiry_thread();

    /**
     * @brief Retrieves a session by its ID without refreshing expiration.
     * @param session_id The unique identifier of the session to retrieve.
     * @return std::optional<session> The session object if found; std::nullopt otherwise.
     */
    std::optional<session> get_session(const std::string& session_id) const;

    /**
     * @brief Adds a backup object to the specified session.
     *
     * Associates the given backup object with the session identified by the provided session ID.
     * If the session does not exist, the operation may fail.
     *
     * @param session_id The unique identifier of the session to which the backup object will be added.
     * @param obj The backup object to add to the session.
     * @return true if the backup object was successfully added to the session; false otherwise.
     */
    bool add_backup_object_to_session(const std::string& session_id, const backup_object& obj);


private:
    std::unordered_map<std::string, session> sessions_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread expiry_thread_;
    bool stop_flag_ = false;
};

} // namespace limestone::grpc::backend
