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

#include "partitioned_cursor_impl.h"
#include <limestone/api/cursor.h>

namespace limestone::internal {

partitioned_cursor_impl::partitioned_cursor_impl(std::shared_ptr<cursor_entry_queue> queue)
    : queue_(std::move(queue)) {}

bool partitioned_cursor_impl::next() {
    if (!queue_) {
        return false;
    }

    // 既存のバッチがあるなら、その続きへ
    if (std::holds_alternative<std::vector<log_entry>>(current_)) {
        const auto& batch = std::get<std::vector<log_entry>>(current_);
        if (current_index_ + 1 < batch.size()) {
            ++current_index_;
            return true;
        }
    }

    // 新しいバッチを取得
    while (true) {
        current_ = queue_->wait_and_pop();
        current_index_ = 0;

        if (std::holds_alternative<end_marker>(current_)) {
            return false;
        }

        const auto& batch = std::get<std::vector<log_entry>>(current_);
        if (!batch.empty()) {
            return true;
        }
        // 空バッチなら次を待つ
    }
}

api::storage_id_type partitioned_cursor_impl::storage() const noexcept {
    return std::get<std::vector<log_entry>>(current_).at(current_index_).storage();
}

void partitioned_cursor_impl::key(std::string& buf) const noexcept {
    std::get<std::vector<log_entry>>(current_).at(current_index_).key(buf);
}

void partitioned_cursor_impl::value(std::string& buf) const noexcept {
    std::get<std::vector<log_entry>>(current_).at(current_index_).value(buf);
}

api::log_entry::entry_type partitioned_cursor_impl::type() const {
    return std::get<std::vector<log_entry>>(current_).at(current_index_).type();
}

std::vector<api::blob_id_type> partitioned_cursor_impl::blob_ids() const {
    return std::get<std::vector<log_entry>>(current_).at(current_index_).get_blob_ids();
}

log_entry& partitioned_cursor_impl::current() {
    return std::get<std::vector<log_entry>>(current_).at(current_index_);
}

void partitioned_cursor_impl::close() {
    queue_.reset();
    current_ = end_marker{};
    current_index_ = 0;
}

std::unique_ptr<api::cursor> limestone::internal::partitioned_cursor_impl::create_cursor(
    std::shared_ptr<limestone::internal::cursor_entry_queue> queue) {
    auto impl = std::make_unique<partitioned_cursor_impl>(std::move(queue));
    return std::unique_ptr<api::cursor>(new api::cursor(std::move(impl)));
}


}  // namespace limestone::internal

