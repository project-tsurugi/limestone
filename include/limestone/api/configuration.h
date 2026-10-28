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
#include <string>
#include <string_view>
#include <vector>

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

    /**
     * @brief default value of sort_arena_size_mb
     */
    static constexpr std::size_t default_sort_arena_size_mb = 256;

    /**
     * @brief default value of sort_merge_fanin
     */
    static constexpr std::size_t default_sort_merge_fanin = 128;

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
     * @brief setter for recover_max_parallelism
     * @param recover_max_parallelism  the number of recover_max_parallelism
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

    int recover_max_parallelism_{default_recover_max_parallelism};

    std::size_t sort_arena_size_mb_{default_sort_arena_size_mb};
    boost::filesystem::path sort_work_dir_{};
    std::size_t sort_merge_fanin_{default_sort_merge_fanin};

    friend class datastore;
};

} // namespace limestone::api
