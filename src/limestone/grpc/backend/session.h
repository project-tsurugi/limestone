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

#include <string>
#include <functional>
#include <atomic>

namespace limestone::grpc::backend {

class session {
public:
    /**
     * @brief Type alias for the on-remove callback function.
     */
    using on_remove_callback_type = std::function<void()>;

    /**
     * @brief Constructor for testing only. Do not use in production code.
     * @param session_id Session ID string.
     * @param expire_at Expiration time (UNIX timestamp).
     * @param on_remove Callback to be called on session remove.
     */
    explicit session(std::string session_id, int64_t expire_at, on_remove_callback_type on_remove = nullptr);

    /**
     * @brief Construct a session with timeout (seconds from now).
     * @param timeout_seconds Timeout in seconds from now.
     * @param on_remove Callback to be called on session remove.
     */
    explicit session(int64_t timeout_seconds, on_remove_callback_type on_remove = nullptr);

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

private:
    static std::string generate_uuid();

    std::string session_id_;
    std::atomic<int64_t> expire_at_;
    on_remove_callback_type on_remove_;
};

} // namespace limestone::grpc::backend
