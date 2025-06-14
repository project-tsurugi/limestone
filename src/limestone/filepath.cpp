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
