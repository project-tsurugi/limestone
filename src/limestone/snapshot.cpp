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
#include <limestone/api/snapshot.h>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "compaction_catalog.h"
#include "logging_helper.h"

namespace limestone::api {  // FIXME fill implementation

snapshot::snapshot(boost::filesystem::path location) noexcept : location_(std::move(location)) {}

std::unique_ptr<cursor> snapshot::get_cursor() const {
    boost::filesystem::path compacted_file = location_ / limestone::internal::compaction_catalog::get_compacted_filename();
    boost::filesystem::path snapshot_file = location_ / std::string(subdirectory_name_) / std::string(file_name_);

    if (boost::filesystem::exists(compacted_file)) {
        return std::unique_ptr<cursor>(new cursor(snapshot_file, compacted_file));
    }
    return std::unique_ptr<cursor>(new cursor(snapshot_file));
}

std::unique_ptr<cursor> snapshot::find([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key) const noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME should implement
}

std::unique_ptr<cursor> snapshot::scan([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key, [[maybe_unused]] bool inclusive) const noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME should implement
}

} // namespace limestone::api
