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
#include <limestone/api/snapshot.h>
#include <limestone/logging.h>

#include <map>

#include "cursor_impl.h"
#include "limestone_exception_helper.h"
#include "logging_helper.h"
#include "partitioned_cursor/cursor_distributor.h"
#include "partitioned_cursor/partitioned_cursor_consts.h"
#include "partitioned_cursor/partitioned_cursor_impl.h"


namespace limestone::internal {

snapshot_impl::snapshot_impl(boost::filesystem::path location,
                             std::map<storage_id_type, write_version_type> clear_storage,
                             std::optional<std::string> compacted_file_name) noexcept
    : location_(std::move(location)), clear_storage(std::move(clear_storage)),
      compacted_file_name_(std::move(compacted_file_name)) {
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

    namespace li = limestone::internal;
    namespace la = limestone::api;

    std::vector<std::shared_ptr<li::cursor_entry_queue>> queues;
    std::vector<std::unique_ptr<la::cursor>> cursors;

    for (std::size_t i = 0; i < n; ++i) {
        auto queue = std::make_shared<li::cursor_entry_queue>(CURSOR_QUEUE_CAPACITY);
        queues.push_back(queue);
        cursors.emplace_back(li::partitioned_cursor_impl::create_cursor(queue));
    }

    boost::filesystem::path snapshot_file = location_ / std::string(la::snapshot::subdirectory_name_) / std::string(la::snapshot::file_name_);

    // As in get_cursor(), the catalog record decides whether a compacted file is present.
    std::unique_ptr<li::cursor_impl_base> base_cursor;
    if (compacted_file_name_.has_value()) {
        boost::filesystem::path compacted_file = location_ / compacted_file_name_.value();
        base_cursor = std::make_unique<li::cursor_impl>(snapshot_file, compacted_file, clear_storage);
    } else {
        base_cursor = std::make_unique<li::cursor_impl>(snapshot_file, clear_storage);
    }

    auto distributor = std::make_shared<li::cursor_distributor>(
        std::move(base_cursor),
        std::move(queues)
    );
    distributor->start();

    return cursors;
}


} // namespace limestone::internal
