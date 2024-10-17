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
#pragma once
#include <memory>
#include <mutex>
#include <map>
#include <optional>
#include "sortdb_wrapper.h" 
#include <limestone/api/write_version_type.h>
#include <limestone/api/storage_id_type.h>

namespace limestone::internal {

using api::sortdb_wrapper;
using api::storage_id_type;
using api::write_version_type;

class sorting_context {
public:
    sorting_context(sorting_context&& obj) noexcept;
    sorting_context(const sorting_context&) = delete;
    sorting_context& operator=(const sorting_context&) = delete;
    sorting_context& operator=(sorting_context&&) = delete;
    sorting_context() = default;
    ~sorting_context() = default;
    explicit sorting_context(std::unique_ptr<sortdb_wrapper>&& s) noexcept;

    // public getter
    sortdb_wrapper* get_sortdb();

    // clear_storage methods
    void clear_storage_update(const storage_id_type sid, const write_version_type wv);
    std::optional<write_version_type> clear_storage_find(const storage_id_type sid);
    std::map<storage_id_type, write_version_type> get_clear_storage() const;

private:
    std::unique_ptr<sortdb_wrapper> sortdb;
    std::mutex mtx_clear_storage;
    std::map<storage_id_type, write_version_type> clear_storage;
};

} // namespace limestone::internal