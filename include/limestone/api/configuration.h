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

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

namespace limestone::api {

class datastore;

/**
 * @brief configuration for datastore
 *
 * A value out of the range stated by a setter makes the construction of the datastore fail with a limestone_exception
 * of initialization_failure.
 */
class configuration {
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
     * @brief setter for instance_id
     * @param instance_id the instance id
     */
    void set_instance_id(std::string_view instance_id) noexcept;

    /**
     * @brief setter for db_name
     * @param db_name the database name
     */
    void set_db_name(std::string_view db_name) noexcept;

    /**
     * @brief setter for recover_max_parallelism: the number of threads of the sort at startup and at compaction
     * @param recover_max_parallelism the number of threads (at least 1)
     */
    void set_recover_max_parallelism(int recover_max_parallelism) noexcept;

    /**
     * @brief setter for sort_arena_size_mb: the size of the buffer per thread of the sort at startup and at
     *        compaction, in MiB
     * @param size_mb the size in MiB (1 to 4096)
     */
    void set_sort_arena_size_mb(std::size_t size_mb) noexcept;

    /**
     * @brief setter for sort_work_dir: the directory under which the sort creates its working directory. A
     *        sorting-XXXXXX is created directly under it for each sort and removed when the sort ends (a crash may
     *        leave it)
     * @param dir the directory. It must exist. If empty, the log directory is used
     */
    void set_sort_work_dir(const std::filesystem::path& dir) noexcept;

    /**
     * @brief setter for sort_merge_fanin: the number of sorted files the sort merges at once
     * @param fanin the fan-in (at least 2)
     */
    void set_sort_merge_fanin(std::size_t fanin) noexcept;

private:
    boost::filesystem::path data_location_{};

    std::string instance_id_{};
    std::string db_name_{};

    // nullopt (an empty path for the work dir) means not set; the datastore fills in the defaults of the sort
    std::optional<int> recover_max_parallelism_{};

    std::optional<std::size_t> sort_arena_size_mb_{};
    boost::filesystem::path sort_work_dir_{};
    std::optional<std::size_t> sort_merge_fanin_{};

    friend class datastore;
};

} // namespace limestone::api
