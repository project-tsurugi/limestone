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
#include "sort/key_sid_deduplicator.h"

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"
#include "log_entry/log_entry_format.h"

namespace limestone::internal {

namespace format = log_entry_format;

bool key_sid_deduplicator::accept(const log_entry_view& entry) {
    if (! format::is_data_entry(entry.type())) {
        LOG_LP(FATAL) << "not valid for entry type " << entry.type();
    }
    const std::string_view key_sid = entry.serialized_key_sid();
    if (last_key_sid_) {
        const int c = format::compare_key_sid(key_sid, *last_key_sid_);
        if (c == 0) {
            return false;
        }
        if (c < 0) {
            LOG_LP(FATAL) << "entries are out of order: key_sid is smaller than the previous one";
        }
    }
    last_key_sid_ = key_sid;  // assigns to the existing string if any (reuses the allocated capacity)
    return true;
}

}  // namespace limestone::internal
