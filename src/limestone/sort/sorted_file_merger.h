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
#include <map>
#include <optional>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

#include "log_entry/log_entry_view.h"
#include "sort/sorting_directory.h"

namespace limestone::internal {

/// range deletions: the greatest write_version per storage. The entries of the same storage with a smaller
/// write_version are excluded from the output
using range_deletions = std::map<limestone::api::storage_id_type, limestone::api::write_version_type>;

/**
 * @brief Merges sorted files.
 *
 * A merge takes the entries out of multiple files, each ordered by key_sid, so that the whole is ordered by key_sid
 * (and by write_version in descending order within the same key_sid).
 * While the number of input files (including the compacted WAL file) exceeds the merge fan-in (the number of inputs
 * opened at once), intermediate merges, each combining fan-in files into a new sorted file, are repeated, and the
 * files used as their inputs are removed. Once the number is within the fan-in, the final merge passes the entries,
 * with duplicates removed and range deletions applied, one by one to the function of the caller.
 * Zero or one input is handled the same way. Every merge removes duplicates. Range deletions are applied only in
 * the final merge.
 */
class sorted_file_merger {
public:
    /// the type of a function that receives the entries read by the merge one by one. Used for the output of
    /// merge() (receiving the result of the final merge) and for recording normal_with_blob of compacted_wal_input.
    /// The view passed refers to the read buffer of the input file and is valid only during the call. A receiver that
    /// needs the contents after the call copies them (for example into an owning log_entry).
    /// An exception thrown by the receiver aborts merge() and propagates to the caller as it is.
    using entry_callback = std::function<void(const log_entry_view&)>;

    /// the compacted WAL file added to the inputs of the final merge
    struct compacted_wal_input {
        /// the file. The marker_begin at its beginning is skipped
        boost::filesystem::path file;
        /// called each time a normal_with_blob is read from this file (including the entries later excluded from the
        /// output by the removal of duplicates or by range deletions). Used for recording for the BLOB GC. Not called
        /// if empty
        entry_callback on_normal_with_blob;
    };

    /**
     * @brief Sets the settings of the merge.
     * @param directory the place of the outputs of the intermediate merges
     * @param merge_fanin the merge fan-in (the number of inputs opened at once). Writes a FATAL log and aborts if it is
     *                    less than 2
     */
    sorted_file_merger(sorting_directory& directory, std::size_t merge_fanin);

    /**
     * @brief Merges all the entries of the input files, and passes each entry of the result to output.
     *
     * One call reads the input files from the beginning to the end, performs the intermediate merges, the final
     * merge, the removal of duplicates and the range deletions, and then returns. The result is not returned as a
     * return value. Each time an entry of the output is decided, output is called with it, so the caller processes
     * the result in output (for example writes it to the output file). output is the only means of receiving the
     * result.
     * The outputs of the intermediate merges are created in directory, and the files used as their inputs are
     * removed. The input files of the final merge are not removed.
     * @param sorted_files the sorted files to merge
     * @param compacted the compacted WAL file added to the inputs of the final merge, or nullopt
     * @param deletions the range deletions: for each storage (storage_id), the write_version at which the whole
     *                  storage was deleted (the greatest write_version of clear_storage or remove_storage). The
     *                  entries of that storage with a smaller write_version are regarded as deleted and are not
     *                  passed to output. Nothing is excluded if empty
     * @param output the function that receives the entries of the result (those that passed the removal of
     *               duplicates and the range deletions) one by one in the order of key_sid. Writes a FATAL log and
     *               aborts if it is empty. Writing to the output file (write_serialized) and the handling of
     *               remove_entry and write_version are expected to be done in this function
     * @throws limestone_io_exception if reading an input, writing an intermediate output or removing an input fails
     */
    void merge(std::vector<boost::filesystem::path> sorted_files, const std::optional<compacted_wal_input>& compacted,
               const range_deletions& deletions, const entry_callback& output);

private:
    /**
     * @brief An intermediate merge (not used for the final merge). Combines the inputs into one new sorted file and
     *        removes the inputs.
     * @param inputs the sorted files to combine
     * @return the new sorted file
     * @throws limestone_io_exception if reading an input, writing the output or removing an input fails
     */
    boost::filesystem::path intermediate_merge(const std::vector<boost::filesystem::path>& inputs);

    sorting_directory* directory_;
    std::size_t merge_fanin_;
};

}  // namespace limestone::internal
