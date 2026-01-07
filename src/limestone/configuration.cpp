/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <limestone/api/configuration.h>

namespace limestone::api {

configuration::configuration() noexcept = default;

configuration::configuration(
    const std::vector<boost::filesystem::path>& data_locations,
    boost::filesystem::path metadata_location) noexcept
    : metadata_location_(std::move(metadata_location)) {
    for (auto&& e : data_locations) {
        data_locations_.emplace_back(e);
    }
}

configuration::configuration(
    const std::vector<boost::filesystem::path>&& data_locations,
    boost::filesystem::path metadata_location) noexcept
    : metadata_location_(std::move(metadata_location)) {
    for (auto&& e : data_locations) {
        data_locations_.emplace_back(e);
    }
}

void configuration::set_data_location(boost::filesystem::path data_location) noexcept {
    data_locations_.clear();
    data_locations_.emplace_back(std::move(data_location));
}

} // namespace limestone::api
