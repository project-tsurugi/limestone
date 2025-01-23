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
#pragma once

#include <boost/filesystem.hpp>

namespace limestone::api {

class blob_file {
private:
    boost::filesystem::path blob_path_;
    bool available_ = false;

public:
    /**
     * @brief Constructor to create a BLOB file instance.
     * @param path Path to the BLOB file.
     * @param available Initial availability status of the BLOB file (default: false).
     */
    explicit blob_file(boost::filesystem::path path, bool available = false);

    /**
     * @brief Retrieves the path to the BLOB file.
     * @returns BLOB file path
     */
    [[nodiscard]] boost::filesystem::path const& path() const noexcept;

    /**
     * @brief Returns whether this BLOB file is available.
     * @return true if this is available
     * @return false otherwise
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief Sets the availability status of the BLOB file.
     * @param available New availability status.
     */
    void set_availability(bool available) noexcept;
};

} // namespace limestone::api