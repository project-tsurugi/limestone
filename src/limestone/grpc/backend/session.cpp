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

namespace limestone::grpc::backend {

session::session(std::string session_id, int64_t expire_at, on_remove_callback_type on_remove)
	: session_id_(std::move(session_id)), expire_at_(expire_at), on_remove_(std::move(on_remove)) {}

session::session(int64_t timeout_seconds, on_remove_callback_type on_remove)
	: session_id_(generate_uuid()), expire_at_(0), on_remove_(std::move(on_remove)) {
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

// session::session(session&& other) noexcept
// 	 : session_id_(std::move(other.session_id_)),
// 		 expire_at_(other.expire_at_.load(std::memory_order_acquire)),
// 		 on_remove_(std::move(other.on_remove_))
// {
// }

// session& session::operator=(session&& other) noexcept {
//     if (this != &other) {
//         session_id_ = std::move(other.session_id_);
//         expire_at_.store(other.expire_at_.load(std::memory_order_acquire), std::memory_order_release);
//         on_remove_ = std::move(other.on_remove_);
//     }
//     return *this;
// }


session::session(const session& other)
	: session_id_(other.session_id_),
	  expire_at_(other.expire_at_.load(std::memory_order_acquire)),
	  on_remove_(other.on_remove_)
{
}


} // namespace limestone::grpc::backend

