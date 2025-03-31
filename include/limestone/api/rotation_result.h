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

#ifndef ROTATION_TASK_H
#define ROTATION_TASK_H

#include <iostream>
#include <future>
#include <vector>
#include <string>
#include <queue>
#include <mutex>
#include <memory>

#include "limestone/api/epoch_id_type.h"
#include "limestone/api/datastore.h"

namespace limestone::api {

class rotation_result {
public:
    explicit rotation_result(epoch_id_type epoch) : epoch_id_(epoch) {}
    
    [[nodiscard]] epoch_id_type get_epoch_id() const;
    [[nodiscard]] const std::set<boost::filesystem::path>& get_rotation_end_files() const;
    void set_rotation_end_files(const std::set<boost::filesystem::path>& files);
    void add_rotated_file(const std::string& filename);

private:
    // A set of filenames that were rotated in this rotation process.
    std::set<std::string> latest_rotated_files_;

    // A set of file paths managed by the datastore at the end of this rotation.
    std::set<boost::filesystem::path> rotation_end_files;

    // The epoch ID at the time of the rotation. Any WAL entries with an epoch ID
    // equal to or greater than this are guaranteed not to be present in the rotated files.
    epoch_id_type epoch_id_;
};

} // namespace limestone::api

#endif // ROTATION_TASK_H
