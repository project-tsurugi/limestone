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
#include "rotation_task.h"

namespace limestone::api {

rotation_result::rotation_result() = default;

rotation_result::rotation_result(std::string file, epoch_id_type epoch) : epoch_id_(epoch) {
    latest_rotated_files_.emplace(std::move(file));
}

// Getter
const std::set<std::string>& rotation_result::get_latest_rotated_files() const {
    return latest_rotated_files_;
}

std::optional<epoch_id_type> rotation_result::get_epoch_id() const {
    return epoch_id_;
}

void rotation_result::set_rotation_end_files(const std::set<boost::filesystem::path>& files) {
    rotation_end_files = files;
}

const std::set<boost::filesystem::path>& rotation_result::get_rotation_end_files() const {
    return rotation_end_files;
}

// merge another rotation_result
void rotation_result::add_rotation_result(const rotation_result& other) {
    latest_rotated_files_.insert(other.latest_rotated_files_.begin(), other.latest_rotated_files_.end());

    // set the maximum of the two epoch_ids
    if (other.epoch_id_.has_value()) {
        if (!epoch_id_.has_value()) {
            epoch_id_ = other.epoch_id_;
        } else {
            epoch_id_ = std::max(epoch_id_.value(), other.epoch_id_.value());
        }
    }
}

rotation_task::rotation_task(datastore& envelope) 
    : envelope_(envelope),  result_future_(result_promise_.get_future()) {}


void rotation_task::rotate() {
    rotation_result final_result;
    for (const auto& lc : envelope_.log_channels_) {
        boost::system::error_code error;
        bool result = boost::filesystem::exists(lc->file_path(), error);
        if (!result || error) {
            continue;  // skip if not exists
        }
        // The following code may seem necessary at first glance, but there is a possibility
        // that files could be appended to before the rotation is complete.
        // In that case, skipping them could result in missing files that should be processed.
        // Therefore, this check is not required and has been commented out.
        //
        // result = boost::filesystem::is_empty(lc->file_path(), error);
        // if (result || error) {
        //     continue;  // skip if empty
        // }
        rotation_result channel_result = lc->do_rotate_file();
        final_result.add_rotation_result(channel_result);
    }
    envelope_.rotate_epoch_file();
    final_result.set_rotation_end_files(envelope_.get_files());

    result_promise_.set_value(final_result);
}

rotation_result rotation_task::wait_for_result() {
    return result_future_.get();
}

void rotation_task_helper::enqueue_task(const std::shared_ptr<rotation_task>& task) {
    std::lock_guard<std::mutex> lock(get_mutex());
    get_tasks().push(task);
}

void rotation_task_helper::attempt_task_execution_from_queue() {
    std::lock_guard<std::mutex> lock(get_mutex());
    if (!get_tasks().empty()) {
        auto task = get_tasks().front();
        get_tasks().pop();
        task->rotate();
    }
}

void rotation_task_helper::clear_tasks() {
    std::lock_guard<std::mutex> lock(get_mutex());
    std::queue<std::shared_ptr<rotation_task>> empty;
    std::swap(get_tasks(), empty);
}

size_t rotation_task_helper::queue_size() {
    std::lock_guard<std::mutex> lock(get_mutex());
    return get_tasks().size();
}

std::queue<std::shared_ptr<rotation_task>>& rotation_task_helper::get_tasks() {
    static std::queue<std::shared_ptr<rotation_task>> tasks_;
    return tasks_;
}

std::mutex& rotation_task_helper::get_mutex() {
    static std::mutex mutex_;
    return mutex_;
}

} // namespace limestone::api
