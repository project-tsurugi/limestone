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
#pragma once

#include <filesystem>


#include <boost/filesystem.hpp>

namespace limestone::api {

class datastore;

/**
 * @brief configuration for datastore
 */
class configuration {
    /**
     * @brief default value of recover_max_parallelism
     */
    static constexpr int default_recover_max_parallelism = 8;

public:
    /**
     * @brief create empty object
     */
    configuration() noexcept;

    /**
     * @brief set a single data location
     * @param data_location the data location
     */
    void set_data_location(const std::filesystem::path& data_location) noexcept;


    /**
     * @brief setter for recover_max_parallelism
     * @param recover_max_parallelism  the number of recover_max_parallelism
     */
    void set_recover_max_parallelism(int recover_max_parallelism) noexcept;

private:
    boost::filesystem::path data_location_{};

    int recover_max_parallelism_{default_recover_max_parallelism};

    friend class datastore;
};

} // namespace limestone::api
