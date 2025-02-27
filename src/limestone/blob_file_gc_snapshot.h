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
#include <memory>
#include <mutex>
#include "log_entry_container.h"
#include "limestone/api/blob_id_type.h"

namespace limestone::internal {

/*
 * blob_file_gc_snapshot
 *
 * This class maintains a snapshot of log entries for blob file garbage collection.
 * It collects entries from multiple threads in two separate groups:
 *
 *   - Low entries container: Contains log entries with write_version below boundary_version.
 *     These entries will be merged, sorted, and deduplicated.
 *
 *   - High entries container: Contains log entries with write_version greater than or equal to boundary_version.
 *     These entries are not processed with merge/sort/duplicate removal and will be directly appended.
 */
class blob_file_gc_snapshot {
public:
    /* 
     * Constructs a blob_file_gc_snapshot with the given boundary_version.
     * @param boundary_version The boundary_version for garbage collection.
     */
    explicit blob_file_gc_snapshot(const write_version_type& boundary_version);

    // Disable copy and move semantics.
    blob_file_gc_snapshot(const blob_file_gc_snapshot&) = delete;
    blob_file_gc_snapshot& operator=(const blob_file_gc_snapshot&) = delete;
    blob_file_gc_snapshot(blob_file_gc_snapshot&&) = delete;
    blob_file_gc_snapshot& operator=(blob_file_gc_snapshot&&) = delete;

    /**
     * @brief Destructor that resets the thread-local containers.
     */
    ~blob_file_gc_snapshot();
    
    /* 
     * Sanitizes and adds a log entry to the snapshot.
     *
     * Only entries of type normal_with_blob are processed.
     * The method clears the payload from the entry’s value_etc (keeping the write_version header)
     * and adds the entry into the appropriate container based on its write_version:
     *   - write_version below boundary_version goes to the low entries container.
     *   - write_version greater than or equal to boundary_version goes to the high entries container.
     *
     * @param entry The log_entry to be processed and potentially added.
     */
    void sanitize_and_add_entry(const log_entry& entry);

    /* 
     * Notifies that the add_entry operations in the current thread are complete,
     * and finalizes (sorts) the low container for later merging.
     * For the high container, no sorting is performed.
     */
    void finalize_local_entries();

    /**
     * @brief Finalizes the snapshot after all entries have been added and returns the snapshot.
     *
     * This method calls the two protected methods finalize_low_entries_impl() and
     * finalize_high_entries_impl() sequentially, and combines their results.
     *
     * @return const log_entry_container& The finalized snapshot of log entries.
     */
    const log_entry_container& finalize_snapshot();
    
    /* 
     * Resets the internal state for a new garbage collection cycle.
     */
    void reset();

    /**
     * @brief Returns the boundary version used for garbage collection.
     * 
     * @return const write_version_type& The boundary version.
     */
    const write_version_type& boundary_version() const;

protected:
    // Protected methods for unit testing.

    /**
     * @brief Finalizes low entries: merges, sorts and removes duplicates.
     *
     * @return log_entry_container The merged low entries container.
     */
    log_entry_container finalize_low_entries_impl();

    /**
     * @brief Finalizes high entries: appends all high container entries.
     *
     * @return log_entry_container The container holding all high entries.
     */
    log_entry_container finalize_high_entries_impl();

private:
    // Thread-local pointer to each thread's low log_entry_container.
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static thread_local std::shared_ptr<log_entry_container> tls_low_container_;

    // Thread-local pointer to each thread's high log_entry_container.
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static thread_local std::shared_ptr<log_entry_container> tls_high_container_;

    // The boundary version for write_version used in garbage collection.
    write_version_type boundary_version_;

    // Final snapshot after merging low containers (with deduplication) and appending high entries.
    log_entry_container aggregated_entries;

    // Global mutex to ensure thread-safe access to thread-local container vectors.
    mutable std::mutex global_mtx_;

    // List of thread-local low containers to be merged into the final snapshot.
    std::vector<std::shared_ptr<log_entry_container>> thread_low_containers_;

    // List of thread-local high containers whose entries are directly appended.
    std::vector<std::shared_ptr<log_entry_container>> thread_high_containers_;
};

} // namespace limestone::internal
