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
     * @brief Retrieves the current snapshot scan mode.
     *
     * @return The current snapshot scan mode.
     */
    snapshot_scan_mode get_snapshot_scan_mode();

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

} // namespace limestone::internal