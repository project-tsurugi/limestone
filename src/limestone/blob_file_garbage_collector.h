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
#include <optional> 

#include "blob_file_resolver.h"
#include "blob_id_container.h"
#include "file_operations.h"

namespace limestone::internal {

/**
 * @brief Enumeration representing the state of the BLOB file garbage collector.
 */
enum class blob_file_gc_state {
    not_started,                                  ///< Initial state, no scan has started.
    scanning_blob_only,                           ///< Scanning BLOB files, snapshot scan has not started.
    scanning_snapshot_only,                       ///< Scanning snapshot, BLOB scan has not started.
    scanning_both,                                ///< Both BLOB and snapshot scans are in progress.
    blob_scan_completed_snapshot_not_started,     ///< BLOB scan completed, snapshot scan has not started.
    blob_scan_completed_snapshot_in_progress,     ///< BLOB scan completed, snapshot scan is in progress.
    snapshot_scan_completed_blob_not_started,     ///< Snapshot scan completed, BLOB scan has not started.
    snapshot_scan_completed_blob_in_progress,     ///< Snapshot scan completed, BLOB scan is in progress.
    cleaning_up,                                  ///< Both scans completed, cleanup process in progress.
    completed,                                    ///< Cleanup completed, no further actions required.
    shutdown                                      ///< Shutdown initiated, terminating all operations.
};

/**
 * @brief Enumeration representing possible events that trigger state transitions.
 */
enum class blob_file_gc_event {
    start_blob_scan,
    start_snapshot_scan,
    complete_blob_scan,
    complete_snapshot_scan,
    complete_cleanup,
    shutdown,
    reset
};

/**
 * @brief Manages the state transitions of the BLOB file garbage collector.
 * 
 * This class ensures that state transitions occur in a valid manner and provides
 * thread-safe access to the current state.
 */
class blob_file_gc_state_machine {
public:
    enum class snapshot_scan_mode {
        none,       // Scan not started yet
        internal,   // BLOB file GC executes scan internally
        external    // Accept scan results from external source
    };

    /**
     * @brief Constructor initializing the state to `not_started`.
     */
    blob_file_gc_state_machine() : current_state_(blob_file_gc_state::not_started) {}

    /**
     * @brief Initiates the BLOB file scan.
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state start_blob_scan();

    /**
     * @brief Initiates the snapshot scan.
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state start_snapshot_scan(snapshot_scan_mode mode);

    /**
     * @brief Marks the BLOB file scan as completed.
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state complete_blob_scan();

    /**
     * @brief Marks the snapshot scan as completed.
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state complete_snapshot_scan(snapshot_scan_mode mode);

    /**
     * @brief Receives updates on the snapshot scan progress.
     * 
     * This method is used to report BLOB IDs discovered during an external snapshot scan.
     * Unlike `start_snapshot_scan()`, this method can be called while another scan is in progress.
     * 
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state notify_snapshot_scan_progress();

    /**
     * @brief Marks the cleanup process as completed.
     * @return The new state after the transition.
     * @throws std::logic_error if the transition is invalid.
     */
    blob_file_gc_state complete_cleanup();

    /**
     * @brief Initiates the shutdown process.
     * @return The new state after the transition.
     */
    blob_file_gc_state shutdown();

    /**
     * @brief Resets the state machine to the initial state.
     *
     * This method resets both the state and snapshot scan mode to their default values.
     */
    blob_file_gc_state  reset();

    /**
     * @brief Retrieves the current state.
     * @return The current state of the garbage collector.
     */
    blob_file_gc_state get_state() const;

    /**
     * @brief Forces the state to a specific value (for testing purposes only).
     *
     * This method is intended for testing and debugging only. It allows setting
     * the state explicitly without following normal state transitions.
     *
     * @param new_state The state to set.
     * @throws std::logic_error if used in production.
     */
    void force_set_state(blob_file_gc_state new_state);

    /**
     * @brief Converts a state enum value to a human-readable string.
     * @param state The state to convert.
     * @return The corresponding string representation.
     */
    static std::string to_string(blob_file_gc_state state);

    /**
     * @brief Generic method to handle state transitions.
     */
    blob_file_gc_state transition(blob_file_gc_event event);


    /**
     * @brief Returns the next state if the transition is valid.
     * 
     * @param current The current state.
     * @param event The event triggering the transition.
     * @return std::optional<blob_file_gc_state> The next state if valid, otherwise std::nullopt.
     */
    std::optional<blob_file_gc_state> get_next_state_if_valid(blob_file_gc_state current, blob_file_gc_event event) const;

    
    /**
     * @brief Converts a blob_file_gc_event enum value to a human-readable string.
     *
     * This method provides a string representation of the given event,
     * making debugging and logging easier.
     *
     * @param event The event to convert.
     * @return A string representing the event.
     */
    static std::string to_string(blob_file_gc_event event);


private:
    blob_file_gc_state current_state_; ///< Stores the current state.
    mutable std::mutex mutex_; ///< Mutex to ensure thread-safe state transitions.
    snapshot_scan_mode snapshot_scan_mode_ = snapshot_scan_mode::none;
};

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
     *
     * @warning This destructor calls shutdown() defensively to prevent resource leaks,
     *          but it is the caller's responsibility to explicitly invoke shutdown()
     *          before destroying the object.
     *          Relying on the destructor to call shutdown() is not recommended,
     *          as it may block execution for an extended period.
     */
    ~blob_file_garbage_collector();

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
     * @brief Adds a BLOB id to the container of BLOBs that are exempt from garbage collection.
     *
     * This method registers a BLOB id which should not be deleted during the garbage collection process.
     *
     * @param id The blob_id to add to the gc_exempt_blob_ container.
     */
    void add_gc_exempt_blob_id(blob_id_type id);

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


    void finalize_scan_snapshot();

    /**
     * @brief Shuts down the garbage collection process.
     *
     * This method performs necessary cleanup and stops all ongoing tasks related
     * to the management and removal of obsolete blob files.
     */
    void shutdown();

    /**
     * Determines if any blob file operations are currently in progress.
     *
     * Returns true if blob file scanning, snapshot scanning, or cleanup has been started.
     * This indicates that the blob file is actively undergoing some form of scanning or cleanup.
     *
     * @return True if any corresponding operation is active; false otherwise.
     */
    bool is_active() const;

    /**
     * @brief Blocks the current thread until all worker threads have completed execution.
     *
     * This function waits for all threads that are performing tasks related to garbage collection,
     * ensuring that no operations are in progress before proceeding. Use this to provide a
     * synchronized point in the program before continuing with operations that depend on the
     * complete termination of background tasks.
     *
     * @warning This function may cause the calling thread to block for an extended period if the
     *          worker threads are processing long-running tasks.
     * @note This function can use test purposes only.
     * 
     */
    void wait_for_all_threads();

protected:
    /**
     * @brief Spawns a background thread that waits for the blob file scan to complete,
     * then performs garbage collection by deleting BLOB files that are not exempt.
     *
     * The deletion targets are determined by computing the difference between the scanned
     * blob ids and those registered as GC-exempt. The background cleanup thread is retained
     * (i.e., not detached) so that it can be joined in shutdown(), ensuring proper termination.
     *
     * This method returns immediately after starting the background thread.
     *
     * @throws std::logic_error if the resolver is not set.
     */
    void finalize_scan_and_cleanup();

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
     * @return const blob_id_container& A constant reference to the container holding the scanned blob ids.
     *
     * @note This method is expected to be called only after the scanning process has completed
     *       (i.e., after wait_for_blob_file_scan() returns). Therefore, no locking is performed here.
     */
    const blob_id_container& get_blob_file_list() const { return *scanned_blobs_; };

    /**
     * @brief Sets a custom file_operations implementation.
     * @param file_ops A unique pointer to the file_operations implementation.
     *
     * Note that this function is intended for testing purposes.
     */
    void set_file_operations(std::unique_ptr<file_operations> file_ops);

    /**
     * @brief Retrieves the list of blob ids that are exempt from garbage collection.
     *
     * @return A constant reference to a container holding the blob ids that are exempt from garbage collection.
     */
    const blob_id_container& get_gc_exempt_blob_list() const { return *gc_exempt_blob_; };


    

private:
    blob_file_gc_state_machine state_machine_;  

    // --- Resolver and Blob Containers ---
    const blob_file_resolver* resolver_ = nullptr;         ///< Pointer to the blob_file_resolver instance.
    std::unique_ptr<blob_id_container> scanned_blobs_;      ///< Container for storing scanned blob ids.
    std::unique_ptr<blob_id_container> gc_exempt_blob_;     ///< Container for storing blob ids exempt from garbage collection.
    blob_id_type max_existing_blob_id_ = 0;                 ///< Maximum blob_id that existed at startup.

    // --- Blob File Scanning Process Fields ---
    std::thread blob_file_scan_thread_;             ///< Background thread for scanning the BLOB directory.
    std::condition_variable blob_file_scan_cv_;     ///< Condition variable to signal blob scan completion.

    // --- Snapshot Scanning Process Fields ---
    std::thread snapshot_scan_thread_;              ///< Background thread for scanning snapshots.
    std::condition_variable snapshot_scan_cv_;      ///< Condition variable to signal snapshot scan completion.

    // --- Cleanup Process Fields ---
    std::thread cleanup_thread_;                    ///< Background thread for garbage collection.
    std::condition_variable cleanup_cv_;            ///< Condition variable to signal cleanup completion.

    // --- Others ---
    mutable std::mutex mutex_;                      ///< Mutex for synchronizing access to state variables.
    std::unique_ptr<file_operations> file_ops_;     ///< Pointer to the file_operations implementation.
    std::mutex shutdown_mutex_;                     ///< Mutex to ensure shutdown() is executed exclusively.
    std::atomic_bool shutdown_requested_{false};    ///< Shutdown flag indicating if shutdown has been requested.

    /**
     * @brief The background function that scans the blob_root directory for BLOB files.
     *
     * This function is executed in a separate thread. It recursively scans the blob_file_resolver's
     * blob root directory for files. For each file, it uses the resolver's is_blob_file() to verify
     * the file format and extract_blob_id() to obtain the blob_id. Only files with blob_id less than or
     * equal to max_existing_blob_id_ are added to scanned_blobs_.
     */
    void scan_directory();

    /**
     * @brief Cleans up internal container resources.
     *
     * This method resets the smart pointers for the scanned blob ids and the GC exempt blob ids
     * by allocating new, empty container instances. This operation effectively releases the memory
     * that was previously allocated to these containers.
     *
     * @note This method is intended to be called after the cleanup process has completed normally,
     *       such as at the end of finalize_scan_and_cleanup() or during shutdown().
     *
     * @warning FIXME: In the case of an exception during processing, ensure that reset() is still
     *          invoked to prevent resource leakage.
     */
    void reset();
};

}  // namespace limestone::internal
