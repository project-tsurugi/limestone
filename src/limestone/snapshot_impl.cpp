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
#include "snapshot_impl.h"

#include <glog/logging.h>
#include <limestone/api/snapshot.h>
#include <limestone/logging.h>

#include <map>

#include "cursor_impl.h"
#include "limestone_exception_helper.h"
#include "locator/locator_format.h"
#include "logging_helper.h"
#include "region_allocator.h"


namespace limestone::internal {

snapshot_impl::snapshot_impl(boost::filesystem::path location,
                             std::map<storage_id_type, write_version_type> clear_storage,
                             std::optional<std::string> compacted_file_name, std::size_t region_records) noexcept
    : location_(std::move(location)), clear_storage(std::move(clear_storage)),
      compacted_file_name_(std::move(compacted_file_name)), region_records_(region_records) {
}

std::unique_ptr<cursor> snapshot_impl::get_cursor() const {
    namespace la = limestone::api;
    boost::filesystem::path snapshot_file = location_ / std::string(la::snapshot::subdirectory_name_) / std::string(la::snapshot::file_name_);

    // Whether a compacted file is present is decided by the catalog record passed to
    // the constructor, never by an implicit fallback on its presence on disk. A record
    // whose file is missing is detected by the startup consistency check.
    if (compacted_file_name_.has_value()) {
        boost::filesystem::path compacted_file = location_ / compacted_file_name_.value();
        return cursor_impl::create_cursor(snapshot_file, compacted_file, clear_storage);
    }
    return cursor_impl::create_cursor(snapshot_file, clear_storage);
}

std::vector<std::unique_ptr<limestone::api::cursor>> snapshot_impl::get_partitioned_cursors(std::size_t n) {
    if (n == 0) {
        throw std::invalid_argument("partition count must be greater than 0");
    }
    if (partitioned_called_.exchange(true)) {
        THROW_LIMESTONE_EXCEPTION("get_partitioned_cursors() was already called once");
    }

    namespace la = limestone::api;
    boost::filesystem::path snapshot_file = location_ / std::string(la::snapshot::subdirectory_name_) / std::string(la::snapshot::file_name_);

    // As in get_cursor(), the catalog record decides whether a compacted file is present.
    std::optional<boost::filesystem::path> compacted_file{};
    std::optional<locator> compacted_locator{};
    if (compacted_file_name_.has_value()) {
        compacted_file = location_ / compacted_file_name_.value();
        compacted_locator = read_locator(*compacted_file);
    }
    std::shared_ptr<region_allocator> allocator =
        std::make_shared<region_allocator>(read_locator(snapshot_file), std::move(compacted_locator), region_records_);

    // open the files of all the cursors here. If a file is removed in the meantime, the cursor that cannot open it
    // throws
    std::vector<std::unique_ptr<la::cursor>> cursors{};
    cursors.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        cursors.push_back(cursor_impl::create_cursor(snapshot_file, compacted_file, clear_storage, allocator));
    }
    return cursors;
}

locator snapshot_impl::read_locator(const boost::filesystem::path& locatee) {
    boost::system::error_code error;
    const boost::uintmax_t size = boost::filesystem::file_size(locatee, error);
    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("cannot get the size of the file: " + locatee.string(), error.value());
    }
    // locator throws limestone_exception if the locator file is invalid
    return locator{locator_format::locator_file_path(locatee), static_cast<std::uint64_t>(size)};
}


} // namespace limestone::internal
