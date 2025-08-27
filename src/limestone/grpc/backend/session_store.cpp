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
#include "session_store.h"
#include <algorithm>
#include <chrono>
#include "limestone/api/epoch_id_type.h"
namespace limestone::grpc::backend {

session_store::session_store() {
    expiry_thread_ = std::thread(&session_store::session_expiry_thread, this);
}

session_store::~session_store() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_flag_ = true;
        cv_.notify_all();
    }
    if (expiry_thread_.joinable()) {
        expiry_thread_.join();
    }
}

std::optional<session> session_store::create_and_register(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds,
                                                          session::on_remove_callback_type on_remove) {
    std::lock_guard<std::mutex> lock(mutex_);
    session s(begin_epoch, end_epoch, timeout_seconds, std::move(on_remove));
    auto [it, inserted] = sessions_.emplace(s.session_id(), s);
    cv_.notify_all();
    if (!inserted) {
        // Collision of session_id (UUID) is practically impossible, so this branch is unreachable in normal operation.
        return std::nullopt;
    }
    return it->second;
}

bool session_store::remove_session(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        it->second.call_on_remove();
        sessions_.erase(it);
        cv_.notify_all();
        return true;
    }
    return false;
}

std::optional<session> session_store::get_and_refresh(const std::string& session_id, int64_t timeout_seconds) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        it->second.refresh(timeout_seconds);
        cv_.notify_all();
        return it->second; 
    }
    return std::nullopt;
}

void session_store::remove_expired_sessions_locked() {
    auto now = static_cast<int64_t>(std::time(nullptr));
    for (auto it = sessions_.begin(); it != sessions_.end(); ) {
        if (it->second.expire_at() <= now) {
            it->second.call_on_remove();
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void session_store::session_expiry_thread() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stop_flag_) {
        auto now = static_cast<int64_t>(std::time(nullptr));
        int64_t next_expire = 0;
        bool has_next = false;
        for (const auto& kv : sessions_) {
            int64_t exp = kv.second.expire_at();
            if (exp <= now) continue;
            if (!has_next || exp < next_expire) {
                next_expire = exp;
                has_next = true;
            }
        }
        remove_expired_sessions_locked();
        if (stop_flag_) break;
        if (!has_next) {
            cv_.wait(lock, [this]{ return stop_flag_ || !sessions_.empty(); });
        } else {
            auto wait_duration = std::chrono::seconds(std::max<int64_t>(1, next_expire - now));
            cv_.wait_for(lock, wait_duration, [this]{ return stop_flag_; });
        }
    }
}

std::optional<session> session_store::get_session(const std::string& session_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool session_store::add_backup_object_to_session(const std::string& session_id, const limestone::backup_object& obj) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
        return false;
    }
    try {
        it->second.add_backup_object(obj);
        return true;
    } catch (...) {
        // e.g., object_id duplication, etc.
        return false;
    }
}

} // namespace limestone::grpc::backend
