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

#include "log_entry.h"

namespace limestone::api {

cursor::cursor(const boost::filesystem::path& file) noexcept : log_entry_(std::make_unique<log_entry>()) {
    istrm_.open(file, std::ios_base::in | std::ios_base::binary );
    if (!istrm_.good()) {
        LOG_LP(ERROR) << "file stream of the cursor is not good (" << file << ")";
        std::abort();
    }
}
cursor::~cursor() noexcept {
    istrm_.close();
}

bool cursor::next() {
    if (!istrm_.good()) {
        DVLOG_LP(log_trace) << "file stream of the cursor is not good";
        return false;
    }
    if (istrm_.eof()) {
        DVLOG_LP(log_trace) << "already detected eof of the cursor";
        return false;
    }
    auto rv = log_entry_->read(istrm_);
    DVLOG_LP(log_trace) << (rv ? "read an entry from the cursor" : "detect eof of the cursor");
    return rv;
}

storage_id_type cursor::storage() const noexcept {
    return log_entry_->storage();
}

void cursor::key(std::string& buf) const noexcept {
    log_entry_->key(buf);
}

void cursor::value(std::string& buf) const noexcept {
    log_entry_->value(buf);
}

std::vector<large_object_view>& cursor::large_objects() noexcept {
    return large_objects_;
}

} // namespace limestone::api
