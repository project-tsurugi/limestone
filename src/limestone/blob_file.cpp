/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <limestone/api/blob_file.h>
#include <boost/filesystem.hpp>

namespace limestone::api {

blob_file::blob_file(boost::filesystem::path path, bool available)
    : blob_path_(std::move(path)), available_(available) {}

boost::filesystem::path const& blob_file::path() const noexcept {
    return blob_path_;
}

blob_file::operator bool() const noexcept {
    return available_;
}

void blob_file::set_availability(bool available) noexcept {
    available_ = available;
}

} // namespace limestone::api
