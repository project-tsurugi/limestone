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
#include "sort/sort_output_writer.h"

#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "log_entry/log_entry_format.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

sort_output_writer::sort_output_writer(boost::filesystem::path file, std::string label, const format& output_format)
    : file_(std::move(file), std::move(label)), format_(output_format) {
    log_entry::begin_session(file_.stream(), 0);
}

void sort_output_writer::write(const log_entry_view& entry) {
    if (! file_.is_open()) {
        LOG_LP(FATAL) << "already closed: " << file_.file();
    }
    if (! log_entry_format::is_data_entry(entry.type())) {
        LOG_LP(FATAL) << "not a data entry: " << entry.type();
    }
    if (entry.type() == log_entry_format::entry_type::remove_entry && ! format_.keep_remove_entry) {
        return;
    }
    log_entry::write_serialized(file_.stream(), entry, format_.write_version);
}

void sort_output_writer::close() {
    if (format_.sync_on_close) {
        file_.sync_and_close();
    } else {
        file_.close();
    }
}

}  // namespace limestone::internal
