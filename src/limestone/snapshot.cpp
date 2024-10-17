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
#include "snapshot_impl.h"

namespace limestone::api {  // FIXME fill implementation

using limestone::internal::snapshot_impl;

snapshot::snapshot(boost::filesystem::path location) noexcept 
    : pimpl(std::make_unique<snapshot_impl>(std::move(location))) {}

std::unique_ptr<cursor> snapshot::get_cursor() const {
    return pimpl->get_cursor();
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
