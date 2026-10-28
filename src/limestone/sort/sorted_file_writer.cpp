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
#include "sort/sorted_file_writer.h"

#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "log_entry.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

sorted_file_writer::sorted_file_writer(boost::filesystem::path file) : file_(std::move(file), "sorted file") {}

sorted_file_writer::~sorted_file_writer() = default;

void sorted_file_writer::write(const log_entry_view& entry) {
    if (! file_.is_open()) {
        LOG_LP(FATAL) << "already closed: " << file_.file();
    }
    if (! deduplicator_.accept(entry)) {
        return;
    }
    log_entry::write_serialized(file_.stream(), entry, log_entry::write_version_handling::keep);
    ++written_count_;
}

void sorted_file_writer::close() {
    file_.close();
}

std::uint64_t sorted_file_writer::written_count() const noexcept {
    return written_count_;
}

}  // namespace limestone::internal
