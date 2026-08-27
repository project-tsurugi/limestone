/*
 * Copyright 2024-2024 Project Tsurugi.
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

#include <glog/logging.h>
#include <limestone/logging.h>

#include <chrono>
#include <iomanip>
#include <sstream>
#include <thread>

#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "internal.h"

namespace limestone::internal {

void remove_trailing_dir_separators(boost::filesystem::path& p) {
    std::string str = p.string();
    std::size_t prev_len{};
    while (true) {
        prev_len = str.size();
        p.remove_trailing_separator();  // remove only one char
        str = p.string();
        if (str.size() >= prev_len) {
            break;
        }
    }
}

bool is_unrotated_pwal_name(std::string_view filename) noexcept {
    constexpr std::size_t unrotated_pwal_name_length = 9;  // "pwal_NNNN"
    return filename.length() == unrotated_pwal_name_length &&
           filename.rfind(log_channel_prefix, 0) == 0;
}

boost::filesystem::path rotate_pwal_file(boost::filesystem::path const& file, epoch_id_type epoch) {
    // Rename-target collision handling: to avoid overwriting an existing rotated
    // file on a second rename of the same file within the same millisecond (or
    // after a clock rollback), re-fetch the wall clock until the target is free.
    // The time is re-fetched rather than incremented so that the timestamp part
    // of the file name always stays the actual wall-clock time.
    boost::filesystem::path new_file;
    while (true) {
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        std::stringstream ss;
        ss << file.filename().string() << "."
           << std::setw(14) << std::setfill('0') << millis
           << "." << epoch;
        new_file = file.parent_path() / ss.str();
        boost::system::error_code exists_ec;
        bool dest_exists = boost::filesystem::exists(new_file, exists_ec);
        if (exists_ec && exists_ec != boost::system::errc::no_such_file_or_directory) {
            LOG_AND_THROW_IO_EXCEPTION("failed to check existence of " + new_file.string(), exists_ec);
        }
        if (!dest_exists) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    boost::system::error_code ec;
    boost::filesystem::rename(file, new_file, ec);
    if (ec) {
        std::string err_msg = "Failed to rename file from " + file.string() + " to " + new_file.string() + ". Error: " + ec.message();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
    return new_file;
}

boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix) {
    auto canonicalpath = boost::filesystem::canonical(target_dir);
    // some versions of boost::filesystem::canonical do not remove trailing directory-separators ('/')
    remove_trailing_dir_separators(canonicalpath);
    std::string targetdirstring = canonicalpath.string();

    auto tmpdirname = targetdirstring + suffix;
    if (::mkdtemp(tmpdirname.data()) == nullptr) {
        LOG_AND_THROW_IO_EXCEPTION("mkdtemp failed", errno);
    }
    return {tmpdirname};
}

}
