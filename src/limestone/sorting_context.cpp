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

void sorting_context::clear_storage_update(const storage_id_type sid, const write_version_type wv) {
    std::unique_lock lk{mtx_clear_storage};
    if (auto [it, inserted] = clear_storage.emplace(sid, wv);
        !inserted) {
        it->second = std::max(it->second, wv);
    }
}

std::optional<write_version_type> sorting_context::clear_storage_find(const storage_id_type sid) {
    auto itr = clear_storage.find(sid);
    if (itr == clear_storage.end()) return {};
    return {itr->second};
}

} // namespace limestone::internal