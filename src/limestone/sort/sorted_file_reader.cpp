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
#include "sort/sorted_file_reader.h"

#include <sstream>
#include <utility>

#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "log_entry/log_entry_format.h"
#include "logging_helper.h"

namespace limestone::internal {

namespace format = log_entry_format;

sorted_file_reader::sorted_file_reader(boost::filesystem::path file, std::uint64_t start_offset)
    : file_(std::move(file)), reader_(file_, start_offset) {}

std::optional<log_entry_view> sorted_file_reader::next() {
    const log_entry_read_result r = reader_.next();
    switch (r.result()) {
    case log_entry_read_result::status::ok: {
        const log_entry_view view = r.view();
        if (! format::is_data_entry(view.type())) {
            std::ostringstream message;
            message << "sorted file has a non-data entry (" << view.type() << ") at offset " << r.offset() << ": "
                    << file_.string();
            LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO(message.str());
        }
        return view;
    }
    case log_entry_read_result::status::end:
        return std::nullopt;
    case log_entry_read_result::status::error: {
        std::ostringstream message;
        message << "sorted file is broken (" << r.error().message() << ") at offset " << r.offset() << ": "
                << file_.string();
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO(message.str());
    }
    }
    LOG_LP(FATAL) << "unexpected read result " << r.result();
    return std::nullopt;
}

}  // namespace limestone::internal
