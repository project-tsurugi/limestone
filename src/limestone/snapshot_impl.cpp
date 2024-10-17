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

#include "snapshot_impl.h"
#include <glog/logging.h>
#include <limestone/logging.h>
#include "compaction_catalog.h"
#include "logging_helper.h"
#include "cursor_impl.h" 
#include <limestone/api/snapshot.h>

namespace limestone::internal {

snapshot_impl::snapshot_impl(boost::filesystem::path location, 
                             std::map<storage_id_type, write_version_type> clear_storage) noexcept
    : location_(std::move(location)), clear_storage(std::move(clear_storage)) {
}

std::unique_ptr<cursor> snapshot_impl::get_cursor() const {
    boost::filesystem::path compacted_file = location_ / limestone::internal::compaction_catalog::get_compacted_filename();
    boost::filesystem::path snapshot_file = location_ / std::string(snapshot::subdirectory_name_) / std::string(snapshot::file_name_);

    if (boost::filesystem::exists(compacted_file)) {
        return cursor_impl::create_cursor(snapshot_file, compacted_file, clear_storage);
    }
    return cursor_impl::create_cursor(snapshot_file, clear_storage);  
}


} // namespace limestone::internal
