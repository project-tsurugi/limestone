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
     * @brief setter for auto_compaction_threshold_mb: 常時コンパクションを実行する WAL の量の閾値 (MiB)。
     *        コンパクションの入力になっていない WAL ファイルの大きさの合計がこの値を超えると、ローテーションと
     *        コンパクションを自動で実行する
     * @param threshold_mb 閾値 (MiB)。0 のときは常時コンパクションを行わない
     */
    void set_auto_compaction_threshold_mb(std::size_t threshold_mb) noexcept;

    /**
     * @brief setter for auto_compaction_delete_detached: 常時コンパクションで、コンパクションの入力になって不要に
     *        なった (detached の) WAL ファイルを削除するか
     * @param enabled 削除するとき true
     */
    void set_auto_compaction_delete_detached(bool enabled) noexcept;

    /**
     * @brief setter for blob_gc: BLOB ファイルの GC を行うか。起動時の GC と、オンラインコンパクションでの GC の
     *        両方に適用される。master とレプリカのどちらにも適用される
     * @param enabled 行うとき true
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
     * @return 常時コンパクションの閾値 (MiB)。未設定のときは std::nullopt
     */
    [[nodiscard]] std::optional<std::size_t> get_auto_compaction_threshold_mb() const noexcept;

    /**
     * @brief getter for auto_compaction_delete_detached
     * @return 常時コンパクションで detached の WAL ファイルを削除するか。未設定のときは std::nullopt
     */
    [[nodiscard]] std::optional<bool> get_auto_compaction_delete_detached() const noexcept;

    /**
     * @brief getter for blob_gc
     * @return BLOB ファイルの GC を行うか。未設定のときは std::nullopt
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

    // nullopt は未設定を表す。未設定の項目は、datastore が既定値で埋める
    std::optional<std::size_t> auto_compaction_threshold_mb_{};
    std::optional<bool> auto_compaction_delete_detached_{};
    std::optional<bool> blob_gc_{};
};

} // namespace limestone::api
