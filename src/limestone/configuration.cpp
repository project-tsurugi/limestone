/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include <limestone/api/configuration.h>

namespace limestone::api {

configuration::configuration() noexcept = default;

void configuration::set_recover_max_parallelism(int recover_max_parallelism) noexcept {
    recover_max_parallelism_ = recover_max_parallelism;
}

void configuration::set_data_location(const std::filesystem::path& data_location) noexcept {
    data_location_ = boost::filesystem::path(data_location.native());
}

void configuration::set_instance_id(std::string_view instance_id) noexcept {
    instance_id_ = instance_id;
}

void configuration::set_db_name(std::string_view db_name) noexcept {
    db_name_ = db_name;
}

void configuration::set_sort_arena_size_mb(std::size_t size_mb) noexcept {
    sort_arena_size_mb_ = size_mb;
}

void configuration::set_sort_work_dir(const std::filesystem::path& dir) noexcept {
    sort_work_dir_ = boost::filesystem::path(dir.native());
}

void configuration::set_sort_merge_fanin(std::size_t fanin) noexcept {
    sort_merge_fanin_ = fanin;
}

} // namespace limestone::api
