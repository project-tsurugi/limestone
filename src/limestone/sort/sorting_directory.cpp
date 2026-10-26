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
#include "sort/sorting_directory.h"

#include <string>

#include <boost/system/error_code.hpp>
#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "logging_helper.h"

namespace limestone::internal {

namespace {

/**
 * @brief Removes a directory with its contents. Does nothing if it does not exist.
 * @param path the directory to remove
 * @throws limestone_io_exception if the removal fails (with errno)
 */
void remove_directory(const boost::filesystem::path& path) {
    boost::system::error_code ec;
    boost::filesystem::remove_all(path, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("cannot remove sorting directory: " + path.string(), ec.value());
    }
}

}  // namespace

sorting_directory::sorting_directory(const boost::filesystem::path& parent) : path_(parent / name) {
    remove_directory(path_);
    boost::system::error_code ec;
    boost::filesystem::create_directory(path_, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("cannot create sorting directory: " + path_.string(), ec.value());
    }
}

sorting_directory::~sorting_directory() {
    if (! removed_) {
        boost::system::error_code ec;
        boost::filesystem::remove_all(path_, ec);  // cleanup without remove(); failure ignored
    }
}

const boost::filesystem::path& sorting_directory::path() const noexcept {
    return path_;
}

boost::filesystem::path sorting_directory::new_file_path() {
    const std::uint64_t number = next_file_number_.fetch_add(1);
    return path_ / ("sorted-" + std::to_string(number));
}

void sorting_directory::remove() {
    if (removed_) {
        LOG_LP(FATAL) << "already removed: " << path_;
    }
    removed_ = true;
    remove_directory(path_);
}

}  // namespace limestone::internal
