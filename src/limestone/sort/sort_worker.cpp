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
#include "sort/sort_worker.h"

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"
#include "sort/sorted_file_writer.h"

namespace limestone::internal {

sort_worker::sort_worker(sorting_directory& directory, std::size_t arena_size_bytes)
    : directory_(&directory), buffer_(arena_size_bytes) {}

void sort_worker::add(const log_entry_view& entry) {
    if (finished_) {
        LOG_LP(FATAL) << "already finished";
    }
    switch (buffer_.append(entry)) {
    case sort_buffer::append_result::appended:
        return;
    case sort_buffer::append_result::full:
        write_sorted_file();
        // the emptied buffer always has room (an entry larger than the arena is excluded as oversized above)
        if (buffer_.append(entry) != sort_buffer::append_result::appended) {
            LOG_LP(FATAL) << "cannot put the entry into the emptied buffer";
        }
        return;
    case sort_buffer::append_result::oversized:
        write_single_file(entry);
        return;
    }
    LOG_LP(FATAL) << "unexpected append result";
}

void sort_worker::finish() {
    if (finished_) {
        LOG_LP(FATAL) << "already finished";
    }
    finished_ = true;
    if (! buffer_.empty()) {
        write_sorted_file();
    }
}

const std::vector<boost::filesystem::path>& sort_worker::sorted_files() const noexcept {
    return sorted_files_;
}

void sort_worker::write_sorted_file() {
    buffer_.sort();
    const boost::filesystem::path file = directory_->new_file_path();
    sorted_file_writer writer(file);
    for (std::size_t i = 0; i < buffer_.entry_count(); ++i) {
        writer.write(buffer_.entry_at(i));  // the writer removes duplicates
    }
    writer.close();
    sorted_files_.push_back(file);
    buffer_.clear();
}

void sort_worker::write_single_file(const log_entry_view& entry) {
    const boost::filesystem::path file = directory_->new_file_path();
    sorted_file_writer writer(file);
    writer.write(entry);
    writer.close();
    sorted_files_.push_back(file);
}

}  // namespace limestone::internal
