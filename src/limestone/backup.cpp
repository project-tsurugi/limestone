/*
 * Copyright 2022-2022 tsurugi project.
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
#include "backup.h"

namespace limestone::api::impl {

backup::backup(std::set<boost::filesystem::path>& files) noexcept {
    for(auto& e : files) {
        files_.emplace_back(e);
    }
}

backup::~backup() noexcept = default;

bool backup::is_ready() const noexcept {
    return true;  // FIXME
}

bool backup::wait_for_ready([[maybe_unused]] std::size_t duration) const noexcept {
    return true;  // FIXME
}

std::vector<boost::filesystem::path>& backup::files() noexcept {
    return files_;
}


} // namespace limestone::api::impl
