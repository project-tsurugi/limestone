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

#include <limestone/api/blob_id_type.h>
#include <boost/filesystem.hpp>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>
#include <memory>

#include "blob_file_resolver.h"
#include "blob_item_container.h"
#include "file_operations.h"

namespace limestone::internal {

/**
 * @brief The blob_file_garbage_collector class is responsible for scanning the BLOB directory,
 *        in a background thread, to generate a list of BLOB file paths for garbage collection.
 *
 * This class uses a blob_file_resolver (provided in the constructor)
 * to obtain the root directory for BLOB files and to utilize its functionality for
 * file name validation and blob_id extraction.
 *
 * Only files whose blob_id is less than or equal to a specified maximum (max_existing_blob_id)
 * are considered for garbage collection. Files with blob_id greater than max_existing_blob_id
 * (i.e., newly generated files) are ignored.
 *
 * This class is intended for internal use only.
 *
 * @note The scanning process is initiated by calling scan_blob_files() exactly once.
 *       Subsequent calls will throw a std::logic_error.
 *
 * @note The wait_for_blob_file_scan() and get_blob_file_list() methods are declared as protected,
 *       as they are intended for internal use (e.g., by derived classes or during testing)
 *       and are only valid to be called after the scanning process has completed.
 *
 * @warning In this implementation, scanned_blobs_ is updated in the background thread
 *          without locking because it is assumed that get_blob_file_list() is invoked only
 *          after wait_for_blob_file_scan() confirms that the scanning is complete.
 *          However, the blob_file_scan_complete_ flag is updated under a mutex to ensure that
 *          the condition variable correctly signals scan completion.
 */
class blob_file_garbage_collector {
public:
    /**
     * @brief Constructor.
     * @param resolver The blob_file_resolver to be used for scanning.
     */
    explicit blob_file_garbage_collector(const blob_file_resolver& resolver);

    /**
     * @brief Destructor.
     */
    ~blob_file_garbage_collector() = default;

    // Disallow copy and move operations.
    blob_file_garbage_collector(const blob_file_garbage_collector&) = delete;
    blob_file_garbage_collector& operator=(const blob_file_garbage_collector&) = delete;
    blob_file_garbage_collector(blob_file_garbage_collector&&) = delete;
    blob_file_garbage_collector& operator=(blob_file_garbage_collector&&) = delete;

    /**
     * @brief Starts scanning the BLOB directory for BLOB files in a background thread.
     *
     * This method launches a separate thread that scans the blob_root directory (and its subdirectories)
     * for files that conform to the expected blob_file naming convention and whose blob_id is less than or
     * equal to the specified max_existing_blob_id. Files with blob_id greater than max_existing_blob_id
     * (i.e., newly generated files) are ignored.
     *
     * @param max_existing_blob_id The maximum blob_id among the BLOB files that existed at startup.
     *
     * @throws std::logic_error if scan_blob_files() is called more than once.
     *
     * @note This function is intended to be called only once during the lifecycle of the object.
     */
    void scan_blob_files(blob_id_type max_existing_blob_id);

    /**
     * @brief Adds a BLOB item to the container of BLOBs that are exempt from garbage collection.
     *
     * This method registers a BLOB item which should not be deleted during the garbage collection process.
     *
     * @param item The blob_item to add to the gc_exempt_blob_ container.
     */
    void add_gc_exempt_blob_item(const blob_item& item);

    /**
     * @brief Starts scanning snapshots in a background thread.
     *
     * This method initiates the snapshot scanning process using the provided snapshot_file and compacted_file.
     * Both files are considered part of the snapshot data.
     *
     * @param snapshot_file The snapshot file.
     * @param compacted_file The compacted file.
     */
    void scan_snapshot(const boost::filesystem::path &snapshot_file, const boost::filesystem::path &compacted_file);

    /**
     * @brief Spawns a background thread that waits for the blob file scan to complete,
     * then performs garbage collection by deleting BLOB files that are not exempt.
     *
     * The deletion targets are determined by computing the difference between the scanned
     * blob items and those registered as GC-exempt. The background cleanup thread is retained
     * (i.e., not detached) so that it can be joined in shutdown(), ensuring proper termination.
     *
     * This method returns immediately after starting the background thread.
     *
     * @throws std::logic_error if the resolver is not set.
     */
    void finalize_scan_and_cleanup();

    /**
     * @brief Shutdown the garbage collector.
     *
     * This method should be called before destroying the object. It waits for any background
     * threads (scanning and cleanup) to finish.
     */
    void shutdown();

protected:
    /**
     * @brief Waits for the background blob file scanning process to complete.
     *
     * This method blocks until the blob file scanning thread sets the blob_file_scan_complete_ flag.
     */
    void wait_for_blob_file_scan();

    /**
     * @brief Waits for the background snapshot scanning process to complete.
     *
     * This method blocks until the snapshot scanning thread sets the snapshot_scan_complete_ flag.
     */
    void wait_for_scan_snapshot();

    /**
     * @brief Waits for the background cleanup thread (spawned by finalize_scan_and_cleanup)
     * to complete.
     *
     * This method blocks until the garbage collection process finishes.
     */
    void wait_for_cleanup();

    /**
     * @brief Retrieves the list of scanned blob files.
     *
     * @return const blob_item_container& A constant reference to the container holding the scanned blob files.
     *
     * @note This method is expected to be called only after the scanning process has completed
     *       (i.e., after wait_for_blob_file_scan() returns). Therefore, no locking is performed here.
     */
    const blob_item_container& get_blob_file_list() const { return scanned_blobs_; };

    /**
     * @brief Sets a custom file_operations implementation.
     * @param file_ops A unique pointer to the file_operations implementation.
     *
     * Note that this function is intended for testing purposes.
     */
    void set_file_operations(std::unique_ptr<file_operations> file_ops);

    /**
     * @brief Retrieves the list of blob items that are exempt from garbage collection.
     *
     * @return A constant reference to a container holding the blob items that are exempt from garbage collection.
     */
    const blob_item_container& get_gc_exempt_blob_list() const { return gc_exempt_blob_; };

private:
    // --- Resolver and Blob Containers ---
    const blob_file_resolver* resolver_ = nullptr;   ///< Pointer to the blob_file_resolver instance.
    blob_item_container scanned_blobs_;              ///< Container for storing scanned blob items.
    blob_item_container gc_exempt_blob_;             ///< Container for storing blob items exempt from garbage collection.
    blob_id_type max_existing_blob_id_ = 0;           ///< Maximum blob_id that existed at startup.

    // --- Blob File Scanning Process Fields ---
    bool blob_file_scan_started_ = false;            ///< Flag indicating whether the blob scanning process has started.
    bool blob_file_scan_complete_ = false;           ///< Flag indicating whether the blob scanning process has completed.
    bool blob_file_scan_waited_ = false;             ///< Flag indicating that wait_for_blob_file_scan() has been called.
    std::thread blob_file_scan_thread_;              ///< Background thread for scanning the BLOB directory.
    std::condition_variable blob_file_scan_cv_;      ///< Condition variable to signal blob scan completion.

    // --- Snapshot Scanning Process Fields ---
    bool snapshot_scan_started_ = false;             ///< Flag indicating whether the snapshot scanning process has started.
    bool snapshot_scan_complete_ = false;            ///< Flag indicating whether the snapshot scanning process has completed.
    bool snapshot_scan_waited_ = false;              ///< Flag indicating that wait_for_scan_snapshot() has been called.
    std::thread snapshot_scan_thread_;               ///< Background thread for scanning snapshots.
    std::condition_variable snapshot_scan_cv_;       ///< Condition variable to signal snapshot scan completion.

    // --- Cleanup Process Fields ---
    bool cleanup_started_ = false;                   ///< Flag indicating whether the cleanup process has started.
    bool cleanup_waited_ = false;                    ///< Flag indicating that wait_for_cleanup() has been called.
    bool cleanup_complete_ = false;                  ///< Flag indicating whether the cleanup process has completed.
    std::thread cleanup_thread_;                     ///< Background thread for garbage collection.
    std::condition_variable cleanup_cv_;             ///< Condition variable to signal cleanup completion.

    // --- Others ---
    mutable std::mutex mutex_;                       ///< Mutex for synchronizing access to state variables.
    std::unique_ptr<file_operations> file_ops_;      ///< Pointer to the file_operations implementation.

    /**
     * @brief The background function that scans the blob_root directory for BLOB files.
     *
     * This function is executed in a separate thread. It recursively scans the blob_file_resolver's
     * blob root directory for files. For each file, it uses the resolver's is_blob_file() to verify
     * the file format and extract_blob_id() to obtain the blob_id. Only files with blob_id less than or
     * equal to max_existing_blob_id_ are added to scanned_blobs_.
     */
    void scan_directory();
};

}  // namespace limestone::internal
