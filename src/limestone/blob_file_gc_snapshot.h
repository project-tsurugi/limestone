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

namespace limestone::internal {

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
     * @brief Destructor that resets the thread-local container.
     */
    ~blob_file_gc_snapshot();
    
    /* 
    * Sanitizes and adds a log entry to the snapshot.
    *
    * Only entries of type normal_with_blob are processed.
    * The method clears the payload from the entry’s value_etc (keeping the write_version header)
    * and adds the entry if its write_version is below the boundary_version.
    *
    * @param entry The log_entry to be processed and potentially added.
    */
    void sanitize_and_add_entry(const log_entry& entry);

    /* 
    * Notifies that the add_entry operations in the current thread are complete,
    * and finalizes (sorts) the local container for later merging.
    */
    void finalize_local_entries();

    /**
     * @brief Finalizes the snapshot after all entries have been added and returns the snapshot.
     *
     * Merges thread-local containers, sorts them in descending order, and removes duplicate entries.
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

private:
    // Thread-local pointer to each thread's log_entry_container.
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
    static thread_local std::shared_ptr<log_entry_container> tls_container_;

    // The boundary version for write_version used in garbage collection.
    write_version_type boundary_version_;

    // Final snapshot after merging, sorting, and duplicate removal.
    log_entry_container snapshot_;

    // Mutex to ensure thread-safe access to internal data.
    mutable std::mutex mtx_;

    // List of thread-local containers to be merged into the final snapshot.
    std::vector<std::shared_ptr<log_entry_container>> thread_containers_;

    // Global mutex to safely access thread_containers_.
    std::mutex global_mtx_;
};

} // namespace limestone::internal
