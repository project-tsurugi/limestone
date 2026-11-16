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
#include "locator/locator_format.h"

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal::locator_format {

std::string locator_file_name(std::string_view locatee_name) {
    return std::string(locatee_name) + std::string(suffix);
}

std::string locatee_file_name(std::string_view locator_name) {
    if (! is_locator_file_name(locator_name)) {
        LOG_LP(FATAL) << "not a locator file name: " << locator_name;
    }
    return std::string(locator_name.substr(0, locator_name.size() - suffix.size()));
}

boost::filesystem::path locator_file_path(const boost::filesystem::path& locatee) {
    return locatee.parent_path() / locator_file_name(locatee.filename().string());
}

bool is_locator_file_name(std::string_view name) {
    return name.size() >= suffix.size() && name.substr(name.size() - suffix.size()) == suffix;
}

}  // namespace limestone::internal::locator_format
