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
#include "blob_gc_entry.h"

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal {

blob_gc_entry::blob_gc_entry(const log_entry_view& entry) {
    if (entry.type() != log_entry_view::entry_type::normal_with_blob) {
        LOG_LP(FATAL) << "not valid for entry type " << entry.type();
    }
    key_sid_ = entry.serialized_key_sid();
    write_version_ = entry.write_version();
    blob_ids_ = entry.blob_ids();
}

std::string_view blob_gc_entry::serialized_key_sid() const noexcept {
    return key_sid_;
}

blob_gc_entry::write_version_type blob_gc_entry::write_version() const noexcept {
    return write_version_;
}

const std::vector<blob_gc_entry::blob_id_type>& blob_gc_entry::blob_ids() const noexcept {
    return blob_ids_;
}

}  // namespace limestone::internal
