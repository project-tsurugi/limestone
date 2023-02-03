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
#include <set>

#include <boost/filesystem/path.hpp>

#include <limestone/api/backup.h>
#include <limestone/api/log_channel.h>

namespace limestone::api::impl {

/**
 * @brief class encapsulating backup operations
 */
class backup : public api::backup {
public:
    /**
     * @brief create empty object
     */
    backup() = default;
    backup(backup const& other) = delete;
    backup& operator=(backup const& other) = delete;
    backup(backup&& other) noexcept = delete;
    backup& operator=(backup&& other) noexcept = delete;

    /**
     * @brief destruct the object
     */
    ~backup() noexcept;

    /**
     * @brief returns whether the current backup operation is available
     * @return true if the current backup operation is available, false otherwise
     */
    [[nodiscard]] bool is_ready() const noexcept override;

    /**
     * @brief wait until backup operation is available
     * @param duration the maximum time to wait
     * @return true if the current backup operation is available, false otherwise
     */
    [[nodiscard]] bool wait_for_ready(std::size_t duration) const noexcept override;

    /**
     * @brief returns a list of files to be backed up
     * @returns a list of files to be backed up
     * @note this operation requires that a backup is available
     */
    std::vector<boost::filesystem::path>& files() noexcept override;

private:
    std::vector<boost::filesystem::path> files_;

    explicit backup(std::set<boost::filesystem::path>& files) noexcept;

    friend class datastore;
};

} // namespace limestone::api::impl
