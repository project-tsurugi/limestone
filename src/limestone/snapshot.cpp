/*
 * Copyright 2022-2022 tsurugi project.
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
#include "logging_helper.h"

#include <boost/filesystem.hpp>

#include "cursor.h"
#include "snapshot.h"

namespace limestone::api::impl {  // FIXME fill implementation

snapshot::snapshot(const boost::filesystem::path& location) noexcept : dir_(location / boost::filesystem::path(std::string(subdirectory_name_))) {
}

snapshot::~snapshot() noexcept = default;

std::unique_ptr<api::cursor> snapshot::get_cursor() const noexcept {
    return std::unique_ptr<cursor>(new cursor(file_path()));
}

std::unique_ptr<api::cursor> snapshot::find([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key) const noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME should implement
}

std::unique_ptr<api::cursor> snapshot::scan([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view entry_key, [[maybe_unused]] bool inclusive) const noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME should implement
}

boost::filesystem::path snapshot::file_path() const noexcept {
    return dir_ / boost::filesystem::path(std::string(file_name_));
}

} // namespace limestone::api::impl
