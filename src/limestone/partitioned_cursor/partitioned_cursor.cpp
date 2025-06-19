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

#include "partitioned_cursor.h"

namespace limestone::internal {

partitioned_cursor::partitioned_cursor(std::shared_ptr<cursor_entry_queue> queue)
    : queue_(std::move(queue)) {}

bool partitioned_cursor::next() {
    if (!queue_) {
        return false;
    }
    current_ = queue_->wait_and_pop();
    return std::holds_alternative<api::log_entry>(current_);
}

api::storage_id_type partitioned_cursor::storage() const noexcept {
    return std::get<api::log_entry>(current_).storage();
}

void partitioned_cursor::key(std::string& buf) const noexcept {
    std::get<api::log_entry>(current_).key(buf);
}

void partitioned_cursor::value(std::string& buf) const noexcept {
    std::get<api::log_entry>(current_).value(buf);
}

api::log_entry::entry_type partitioned_cursor::type() const {
    return std::get<api::log_entry>(current_).type();
}

std::vector<api::blob_id_type> partitioned_cursor::blob_ids() const {
    return std::get<api::log_entry>(current_).get_blob_ids();
}

void partitioned_cursor::close() {
    queue_.reset();
}

}  // namespace limestone::internal
