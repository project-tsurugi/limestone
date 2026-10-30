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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/blob_id_type.h>
#include <limestone/api/epoch_id_type.h>

#include "dblog_scan.h"
#include "log_entry/log_entry.h"
#include "sort/sort_settings.h"
#include "sort/sort_worker.h"
#include "sort/sorted_file_merger.h"
#include "sort/sorting_directory.h"

namespace limestone::internal {

/// the handling of the snippets whose epoch exceeds the boundary epoch of the scan. In both cases, their entries are
/// not included in the sort
enum class beyond_boundary_handling {
    /// rewrites the marker to invalidated (dblog_scan::scan_pwal_files_throws()). Used by the scans that discard the
    /// snippets not made durable (at startup, and the offline compaction)
    invalidate,
    /// skips them without modifying the files (dblog_scan::scan_pwal_files_for_compaction()). Used by the scan that
    /// leaves the snippets to be carried over into the carry file later (the online compaction)
    skip,
};

/// the result of the scan
struct wal_scan_result {
    /// the greatest epoch the scan saw (the return value of dblog_scan)
    limestone::api::epoch_id_type max_appeared_epoch{};
    /// the greatest ID of the BLOBs referred to by normal_with_blob (including the entries excluded by the removal of
    /// duplicates and by the range deletions). 0 if none
    limestone::api::blob_id_type max_blob_id{};
    /// the range deletions: the greatest write_version of clear_storage and remove_storage per storage
    range_deletions deletions{};
};

/**
 * @brief One sort of WAL files. Drives the scan (the sort and the writing of sorted files per thread) and the final
 *        merge.
 *
 * Used as scan() then merge(), once each. The caller opens the output file between scan() and merge(), and writes
 * the result in the output of merge(). If scan() fails, merge() cannot be called, and sorting is removed by the
 * destructor.
 */
class wal_sorter {
public:
    using entry_callback = sorted_file_merger::entry_callback;
    /// the type of a function that receives an owning entry. Called in a thread of the scan. The entry is valid only
    /// during the call
    using owned_entry_callback = std::function<void(const limestone::api::log_entry&)>;

    /**
     * @brief Receives the target of the scan and the settings. sorting is not created yet.
     * @param scan the dblog_scan with the target of the scan (the directory and the set of files) configured. The
     *             number of threads and the thread end hook are set by scan(). It must be valid until this
     *             wal_sorter is destroyed
     * @param settings the settings of the sort. Writes a FATAL log and aborts if threads is less than 1
     *                 (arena_size_bytes and merge_fanin are checked by sort_buffer and sorted_file_merger)
     * @param work_dir_parent the parent directory in which sorting is created. It must exist
     */
    wal_sorter(dblog_scan& scan, const sort_settings& settings, boost::filesystem::path work_dir_parent);

    /**
     * @brief Removes sorting if it remains (a failure is ignored).
     */
    ~wal_sorter() = default;

    wal_sorter(const wal_sorter&) = delete;
    wal_sorter& operator=(const wal_sorter&) = delete;
    wal_sorter(wal_sorter&&) = delete;
    wal_sorter& operator=(wal_sorter&&) = delete;

    /**
     * @brief The scan: creates sorting, scans the WAL files, and sorts and writes out the entries to sorted files per
     *        worker.
     *
     * Called only once (writes a FATAL log and aborts if called twice). A data entry goes into the buffer of the
     * worker that read it. clear_storage and remove_storage are recorded as range deletions, and the greatest BLOB
     * ID of normal_with_blob is recorded. add_storage is ignored.
     * @param boundary_epoch the boundary epoch of the scan. The snippets whose epoch exceeds it are not included in
     *                       the sort (the durable epoch at startup and in the offline compaction, the rotation
     *                       boundary in the online compaction)
     * @param handling the handling of the snippets beyond the boundary
     * @param on_normal_with_blob called each time a normal_with_blob is read, in the thread of the scan, before the
     *                            removal of duplicates (for recording for the BLOB GC). May be called concurrently
     *                            from multiple threads. Not called if empty
     * @return the result of the scan
     * @throws limestone_exception if creating sorting, the scan, or writing a sorted file fails
     */
    wal_scan_result scan(limestone::api::epoch_id_type boundary_epoch, beyond_boundary_handling handling,
                         const owned_entry_callback& on_normal_with_blob);

    /**
     * @brief The final merge: merges the sorted files (and the compacted WAL file if any), and passes the result,
     *        with duplicates removed and the range deletions applied, to output one by one. Removes sorting at the
     *        end.
     *
     * Called only once after a successful scan() (writes a FATAL log and aborts if called otherwise or twice).
     * @param compacted the compacted WAL file added to the inputs of the final merge, or nullopt
     * @param output the function that receives the entries of the result one by one in the order of key_sid. The
     *               view passed is valid only during the call
     * @throws limestone_io_exception if the merge or the removal of sorting fails
     */
    void merge(const std::optional<sorted_file_merger::compacted_wal_input>& compacted, const entry_callback& output);

private:
    /**
     * @brief Dispatches one entry read by the scan by its type (called in a thread of the scan).
     * @param entry the entry read
     * @param worker the number of the worker that read it
     * @param on_normal_with_blob the argument of scan()
     */
    void add_entry(const limestone::api::log_entry& entry, int worker,
                   const owned_entry_callback& on_normal_with_blob);

    dblog_scan* scan_;
    sort_settings settings_;
    boost::filesystem::path work_dir_parent_;
    std::unique_ptr<sorting_directory> directory_{};
    std::vector<std::unique_ptr<sort_worker>> workers_{};
    std::vector<boost::filesystem::path> sorted_files_{};
    std::mutex deletions_mutex_{};
    range_deletions deletions_{};
    /// the greatest BLOB ID per worker (indexed by the worker number). Each worker updates only its own slot
    std::vector<limestone::api::blob_id_type> max_blob_ids_{};
    bool scanned_{};
    bool merged_{};
};

}  // namespace limestone::internal
