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

#include <cstddef>
#include <vector>
#include "log_entry_container.h"
#include "limestone/api/blob_id_type.h"

namespace limestone {
namespace internal {

/*
 * blob_id_iterator
 *
 * A simple, Java-style iterator that allows sequential access to all blob IDs
 * contained in a snapshot (log_entry_container) without creating a separate list.
 * 
 * The iterator provides:
 *   - has_next(): to check if there is a next blob ID.
 *   - current(): to retrieve the current blob ID and advance the iterator.
 *
 * Internally, the iterator caches the blob IDs from the current log entry to
 * avoid repeated calls to get_blob_ids().
 */
class blob_id_iterator {
public:
    // Constructs an iterator for the given snapshot.
    // The iterator starts at the first blob ID of the first log entry.
    explicit blob_id_iterator(const log_entry_container* snapshot);

    // Returns true if there is a next blob ID in the snapshot.
    bool has_next() const;

    // Returns the current blob ID and advances the iterator.
    // Caller should check has_next() before calling current().
    limestone::api::blob_id_type current();

private:
    // Pointer to the snapshot containing log entries.
    const log_entry_container* snapshot_;
    // Index of the current log entry within the snapshot.
    std::size_t entry_index_;
    // Index of the current blob ID within the blob ID list of the current log entry.
    std::size_t blob_index_;
    // Cached blob IDs from the current log entry to reduce repeated retrieval.
    std::vector<limestone::api::blob_id_type> cached_blob_ids_;

    // Advances internal indices (and updates the cache) to point to the next valid blob ID.
    // If the current log entry has no further blob IDs, it advances to the next log entry.
    void advance_to_valid();
};

/*
 * blob_file_gc_snapshot
 *
 * This class maintains a snapshot of log entries for blob file garbage collection.
 * It provides an interface to obtain an iterator over all blob IDs contained in the snapshot.
 *
 * Instead of returning a complete list of blob IDs (which may consume significant memory),
 * the class returns a custom iterator that extracts blob IDs on-the-fly.
 */
class blob_file_gc_snapshot {
public:
    /* 
     * Constructs a blob_file_gc_snapshot with the given threshold.
     * @param threshold The write_version threshold for garbage collection.
     */
    explicit blob_file_gc_snapshot(const write_version_type& threshold);

    // Disable copy and move semantics.
    blob_file_gc_snapshot(const blob_file_gc_snapshot&) = delete;
    blob_file_gc_snapshot& operator=(const blob_file_gc_snapshot&) = delete;
    blob_file_gc_snapshot(blob_file_gc_snapshot&&) = default;
    blob_file_gc_snapshot& operator=(blob_file_gc_snapshot&&) = default;

    /* 
    * Sanitizes and adds a log entry to the snapshot.
    *
    * Only entries of type normal_with_blob are processed.
    * The method clears the payload from the entry’s value_etc (keeping the write_version header)
    * and adds the entry if its write_version is below the threshold.
    *
    * @param entry The log_entry to be processed and potentially added.
    */
    void sanitize_and_add_entry(const log_entry& entry);

    /* 
    * Notifies that the add_entry operations in the current thread are complete,
    * and finalizes (sorts) the local container for later merging.
    */
    void finalize_local_entries();


    /* 
     * Finalizes the snapshot after all threads have finished adding entries.
     *
     * This method merges all entries from the internal buffer, sorts them in descending order,
     * and removes duplicate entries with the same key_sid by retaining only the entry with the maximum write_version.
     */
    void finalize_snapshot();

    /* 
     * Returns a blob_id_iterator that provides sequential access to all blob IDs in the snapshot.
     *
     * This iterator allows clients to iterate over each blob ID without creating an additional list,
     * thereby reducing memory overhead.
     */
    blob_id_iterator blob_ids_iterator() const;
    
    /* 
     * Resets the internal state for a new garbage collection cycle.
     */
    void reset();

private:
    /* 
     * Merges entries from multiple sources (if applicable).
     */
    void merge_entries();

    /* 
     * Removes duplicate entries by retaining only the entry with the highest write_version for each key_sid.
     */
    void remove_duplicate_entries();

    // The threshold for write_version used in garbage collection.
    write_version_type threshold_;

    // Internal buffer to accumulate log entries from multiple threads.
    std::vector<log_entry> entries_;

    // Final snapshot after merging, sorting, and duplicate removal.
    log_entry_container snapshot_;

    // Mutex to ensure thread-safe access to internal data.
    mutable std::mutex mtx_;

    std::vector<log_entry_container*> thread_containers_;
    std::mutex global_mtx_;
};

} // namespace internal
} // namespace limestone
