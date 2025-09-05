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
#include "session.h"

#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <ctime>
#include <optional>
#include <stdexcept>

namespace limestone::grpc::backend {

session::session(std::string session_id, epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t expire_at, on_remove_callback_type on_remove)
	: session_id_(std::move(session_id)), begin_epoch_(begin_epoch), end_epoch_(end_epoch), expire_at_(expire_at), on_remove_(std::move(on_remove)) {}

session::session(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, on_remove_callback_type on_remove)
	: session_id_(generate_uuid()), begin_epoch_(begin_epoch), end_epoch_(end_epoch), expire_at_(0), on_remove_(std::move(on_remove)) {
	refresh(timeout_seconds);
}

void session::refresh(int64_t timeout_seconds) {
	expire_at_.store(static_cast<int64_t>(std::time(nullptr)) + timeout_seconds, std::memory_order_release);
}

std::string session::generate_uuid() {
	static thread_local boost::uuids::random_generator uuid_gen;
	boost::uuids::uuid uuid = uuid_gen();
	return boost::uuids::to_string(uuid);
}

const std::string& session::session_id() const {
	return session_id_;
}

int64_t session::expire_at() const {
	return expire_at_.load(std::memory_order_acquire);
}

void session::call_on_remove() {
	if (on_remove_) on_remove_();
}

session::session(const session& other)
    : session_id_(other.session_id_),
      begin_epoch_(other.begin_epoch_),
      end_epoch_(other.end_epoch_),
      expire_at_(other.expire_at_.load(std::memory_order_acquire)),
      on_remove_(other.on_remove_),
      backup_objects_([&other]{
          std::lock_guard<std::mutex> lock(other.backup_objects_mutex_);
          return other.backup_objects_;
      }())
{
}

epoch_id_type session::begin_epoch() const {
	return begin_epoch_;
}

epoch_id_type session::end_epoch() const {
	return end_epoch_;
}

void session::add_backup_object(const backup_object& obj) {
	std::lock_guard<std::mutex> lock(backup_objects_mutex_);
	auto [it, inserted] = backup_objects_.emplace(obj.object_id(), obj);
	if (!inserted) {
		throw std::runtime_error("backup_object with the same object_id already exists");
	}
}

std::optional<backup_object> session::find_backup_object(const std::string& object_id) const {
	std::lock_guard<std::mutex> lock(backup_objects_mutex_);
	auto it = backup_objects_.find(object_id);
	if (it != backup_objects_.end()) {
		return it->second; // コピーを返す
	}
	return std::nullopt;
}

auto session::begin() const noexcept -> std::map<std::string, backup_object>::const_iterator {
	return backup_objects_.cbegin();
}

auto session::end() const noexcept -> std::map<std::string, backup_object>::const_iterator {
	return backup_objects_.cend();
}

} // namespace limestone::grpc::backend

