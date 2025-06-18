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

#include <limestone/api/datastore.h>
#include <limestone/api/rotation_result.h>

namespace limestone::api {

epoch_id_type rotation_result::get_epoch_id() const {
    return epoch_id_;
}

void rotation_result::set_rotation_end_files(const std::set<boost::filesystem::path>& files) {
    rotation_end_files = files;
}

const std::set<boost::filesystem::path>& rotation_result::get_rotation_end_files() const {
    return rotation_end_files;
}

void rotation_result::add_rotated_file(const std::string& filename) {
    latest_rotated_files_.insert(filename);
}

} // namespace limestone::api
