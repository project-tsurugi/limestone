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
#include <map>
#include "sorting_context.h"
#include <algorithm> 

namespace limestone::internal {

sorting_context::sorting_context(sorting_context&& obj) noexcept : sortdb(std::move(obj.sortdb)) {
    std::unique_lock lk{obj.mtx_clear_storage};
    clear_storage = std::move(obj.clear_storage);  // NOLINT(*-prefer-member-initializer): need lock
}

sorting_context::sorting_context(std::unique_ptr<sortdb_wrapper>&& s) noexcept : sortdb(std::move(s)) {
}

sortdb_wrapper* sorting_context::get_sortdb() {
    return sortdb.get();
}

void sorting_context::clear_storage_update(storage_id_type sid, write_version_type wv) {
    std::unique_lock lk{mtx_clear_storage};
    if (auto [it, inserted] = clear_storage.emplace(sid, wv);
        !inserted) {
        it->second = std::max(it->second, wv);
    }
}

std::optional<write_version_type> sorting_context::clear_storage_find(storage_id_type sid) {
    auto itr = clear_storage.find(sid);
    if (itr == clear_storage.end()) return {};
    return {itr->second};
}

std::map<storage_id_type, write_version_type> sorting_context::get_clear_storage() const {
    return clear_storage;
}

void sorting_context::update_max_blob_id(const std::vector<blob_id_type>& blob_ids) {
    if (blob_ids.empty()) {
        return;
    }
    // Find the maximum value in blob_ids
    blob_id_type new_max = *std::max_element(blob_ids.begin(), blob_ids.end());
    // Load the current maximum blob ID with relaxed memory order
    blob_id_type current = max_blob_id_.load(std::memory_order_relaxed);
    // Try to update max_blob_id_ if new_max is greater than current
    while (current < new_max &&
           // Attempt to update max_blob_id_ to new_max
           // If another thread updates max_blob_id_ first, current is updated to the latest value
           !max_blob_id_.compare_exchange_weak(current, new_max, std::memory_order_relaxed)) {
        // Loop continues until max_blob_id_ is successfully updated or current >= new_max
    }
}

blob_id_type sorting_context::get_max_blob_id() const {
    return max_blob_id_.load(std::memory_order_relaxed);
}

} // namespace limestone::internal