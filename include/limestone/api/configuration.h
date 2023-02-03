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
#pragma once

#include <vector>

#include <boost/filesystem/path.hpp>

namespace limestone::api {

class datastore;

/**
 * @brief configuration for datastore
 */
class configuration {
public:
    /**
     * @brief create empty object
     */
    configuration();

    /**
     * @brief create a object
     * @param data_locations the list of data locations
     * @param metadata_location the location of the metadata
     */
    configuration(const std::vector<boost::filesystem::path>& data_locations, boost::filesystem::path metadata_location) noexcept;

    /**
     * @brief create a object
     * @param data_locations the list of data locations
     * @param metadata_location the location of the metadata
     */
    configuration(const std::vector<boost::filesystem::path>&& data_locations, boost::filesystem::path metadata_location) noexcept;

    std::vector<boost::filesystem::path> data_locations() const noexcept { return data_locations_; }

    boost::filesystem::path metadata_location() const noexcept { return metadata_location_; };

private:
    std::vector<boost::filesystem::path> data_locations_{};

    boost::filesystem::path metadata_location_{};

};

} // namespace limestone::api
