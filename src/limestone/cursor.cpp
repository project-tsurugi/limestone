/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <limestone/api/cursor.h>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "snapshot_tracker.h"


namespace limestone::api {


cursor::cursor(const boost::filesystem::path& snapshot_file)
    : log_entry_tracker_(std::make_unique<limestone::internal::snapshot_tracker>(snapshot_file)) {}

cursor::cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file)
    : log_entry_tracker_(std::make_unique<limestone::internal::snapshot_tracker>(snapshot_file, compacted_file)) {}

cursor::~cursor() noexcept {
    // TODO: handle close failure
    log_entry_tracker_->close();
}

bool cursor::next() {
    return log_entry_tracker_->next();
}

storage_id_type cursor::storage() const noexcept {
    return log_entry_tracker_->storage();
}

void cursor::key(std::string& buf) const noexcept {
    log_entry_tracker_->key(buf);
}

void cursor::value(std::string& buf) const noexcept {
    log_entry_tracker_->value(buf);
}

std::vector<large_object_view>& cursor::large_objects() noexcept {
    return large_objects_;
}

} // namespace limestone::api
