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

#include <vector>
#include <set>

#include <boost/filesystem.hpp>

#include <limestone/api/log_channel.h>

namespace limestone::api {

class datastore;

/**
 * @brief class encapsulating backup operations
 */
class backup {
public:
    /**
     * @brief destruct the object
     */
    ~backup() noexcept;

    backup(backup const& other) = delete;
    backup& operator=(backup const& other) = delete;
    backup(backup&& other) noexcept = delete;
    backup& operator=(backup&& other) noexcept = delete;

    /**
     * @brief returns whether the current backup operation is available
     * @return true if the current backup operation is available, false otherwise
     */
    [[nodiscard]] bool is_ready() const noexcept;

    /**
     * @brief wait until backup operation is available
     * @param duration the maximum time to wait
     * @return true if the current backup operation is available, false otherwise
     */
    [[nodiscard]] bool wait_for_ready(std::size_t duration) const noexcept;

    /**
     * @brief returns a list of files to be backed up
     * @returns a list of files to be backed up
     * @note this operation requires that a backup is available
     */
    std::vector<boost::filesystem::path>& files() noexcept;

    /**
     * @brief Notifies that the backup process has ended.
     *
     * This method must be called after the backup process is complete and before the
     * backup object is destroyed. It is required to call this method regardless of the
     * outcome of the backup.
     */
    void notify_end_backup() noexcept;

private:
    std::vector<boost::filesystem::path> files_;

    explicit backup(const std::set<boost::filesystem::path>& files) noexcept;

    friend class datastore;
};

} // namespace limestone::api
