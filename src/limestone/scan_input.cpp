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
#include "scan_input.h"

#include <utility>

namespace limestone::internal {

scan_input::scan_input(boost::filesystem::path path) : path_(std::move(path)) {}

scan_input::scan_input(boost::filesystem::path path, const std::atomic<bool>& stop)
    : path_(std::move(path)), stop_(&stop) {}

const boost::filesystem::path& scan_input::path() const noexcept {
    return path_;
}

bool scan_input::stopped() const noexcept {
    return stop_ != nullptr && stop_->load();
}

}  // namespace limestone::internal
