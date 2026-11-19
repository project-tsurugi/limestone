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
#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

namespace limestone::api {

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

    /**
     * @brief setter for auto_compaction_threshold_mb: the threshold of the amount of the WAL at which the auto
     *        compaction runs, in MiB. When the total size of the WAL files that have not been the input of a
     *        compaction exceeds this value, a rotation and a compaction run automatically
     * @param threshold_mb the threshold in MiB. 0 disables the auto compaction
     */
    void set_auto_compaction_threshold_mb(std::size_t threshold_mb) noexcept;

    /**
     * @brief setter for auto_compaction_delete_detached: whether the auto compaction removes the WAL files that
     *        have been the input of a compaction and are no longer needed (the detached WAL files)
     * @param enabled true to remove them
     */
    void set_auto_compaction_delete_detached(bool enabled) noexcept;

    /**
     * @brief setter for blob_gc: whether the BLOB file GC runs. It applies to both the GC at startup and the GC in
     *        an online compaction, on the master and on the replica
     * @param enabled true to run the GC
     */
    void set_blob_gc(bool enabled) noexcept;

    /**
     * @brief getter for data_location
     * @return the data location
     */
    [[nodiscard]] const boost::filesystem::path& get_data_location() const noexcept;

    /**
     * @brief getter for instance_id
     * @return the instance id
     */
    [[nodiscard]] const std::string& get_instance_id() const noexcept;

    /**
     * @brief getter for db_name
     * @return the database name
     */
    [[nodiscard]] const std::string& get_db_name() const noexcept;

    /**
     * @brief getter for recover_max_parallelism
     * @return the number of threads of the sort, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<int> get_recover_max_parallelism() const noexcept;

    /**
     * @brief getter for sort_arena_size_mb
     * @return the size of the buffer per thread of the sort in MiB, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<std::size_t> get_sort_arena_size_mb() const noexcept;

    /**
     * @brief getter for sort_work_dir
     * @return the directory under which the sort creates its working directory, or an empty path if not set
     */
    [[nodiscard]] const boost::filesystem::path& get_sort_work_dir() const noexcept;

    /**
     * @brief getter for sort_merge_fanin
     * @return the fan-in of the merge of the sort, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<std::size_t> get_sort_merge_fanin() const noexcept;

    /**
     * @brief getter for auto_compaction_threshold_mb
     * @return the threshold of the auto compaction in MiB, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<std::size_t> get_auto_compaction_threshold_mb() const noexcept;

    /**
     * @brief getter for auto_compaction_delete_detached
     * @return whether the auto compaction removes the detached WAL files, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<bool> get_auto_compaction_delete_detached() const noexcept;

    /**
     * @brief getter for blob_gc
     * @return whether the BLOB file GC runs, or std::nullopt if not set
     */
    [[nodiscard]] std::optional<bool> get_blob_gc() const noexcept;

private:
    boost::filesystem::path data_location_{};

    std::string instance_id_{};
    std::string db_name_{};

    // nullopt (an empty path for the work dir) means not set; the datastore fills in the defaults of the sort
    std::optional<int> recover_max_parallelism_{};

    std::optional<std::size_t> sort_arena_size_mb_{};
    boost::filesystem::path sort_work_dir_{};
    std::optional<std::size_t> sort_merge_fanin_{};

    // nullopt means that the item is not set; the datastore fills in the items not set with the defaults
    std::optional<std::size_t> auto_compaction_threshold_mb_{};
    std::optional<bool> auto_compaction_delete_detached_{};
    std::optional<bool> blob_gc_{};
};

} // namespace limestone::api
