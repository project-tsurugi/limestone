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
 
 #include "blob_file_garbage_collector.h"
 #include "logging_helper.h"
 #include "cursor_impl.h"
 #include "log_entry.h"
 
 #include <boost/filesystem.hpp>
 #include <sstream>
 #include <iomanip>
 #include <iostream>
 #include <stdexcept>
 #include <exception>
 #include <pthread.h>
 #include <unordered_map>

namespace std {
template <>
struct hash<std::pair<limestone::internal::blob_file_gc_state, limestone::internal::blob_file_gc_event>> {
    size_t operator()(const std::pair<limestone::internal::blob_file_gc_state, limestone::internal::blob_file_gc_event>& pair) const noexcept {
        return std::hash<int>()(static_cast<int>(pair.first)) ^ (std::hash<int>()(static_cast<int>(pair.second)) << 1);
    }
};
}  // namespace std

 namespace limestone::internal {
    
/**
 * @brief Defines a key for the state transition map.
 */
using state_event_pair = std::pair<blob_file_gc_state, blob_file_gc_event>;

static const std::unordered_map<state_event_pair, blob_file_gc_state> state_transition_map = {
    // =========================
    // Not Started
    // =========================
    {{blob_file_gc_state::not_started, blob_file_gc_event::start_blob_scan}, blob_file_gc_state::scanning_blob_only},
    {{blob_file_gc_state::not_started, blob_file_gc_event::start_snapshot_scan}, blob_file_gc_state::scanning_snapshot_only},

    // =========================
    // Scanning Blob Only
    // =========================
    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::start_snapshot_scan}, blob_file_gc_state::scanning_both},
    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::blob_scan_completed_snapshot_not_started},

    // =========================
    // Scanning Snapshot Only
    // =========================
    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::start_blob_scan}, blob_file_gc_state::scanning_both},
    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::snapshot_scan_completed_blob_not_started},

    // =========================
    // Scanning Both
    // =========================
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::blob_scan_completed_snapshot_in_progress},
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::snapshot_scan_completed_blob_in_progress},

    // =========================
    // Blob Scan Completed (Snapshot Not Started)
    // =========================
    {{blob_file_gc_state::blob_scan_completed_snapshot_not_started, blob_file_gc_event::start_snapshot_scan}, blob_file_gc_state::blob_scan_completed_snapshot_in_progress},
    {{blob_file_gc_state::blob_scan_completed_snapshot_not_started, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::cleaning_up},

    // =========================
    // Snapshot Scan Completed (Blob Not Started)
    // =========================
    {{blob_file_gc_state::snapshot_scan_completed_blob_not_started, blob_file_gc_event::start_blob_scan}, blob_file_gc_state::snapshot_scan_completed_blob_in_progress},
    {{blob_file_gc_state::snapshot_scan_completed_blob_not_started, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::cleaning_up},

    // =========================
    // Blob Scan Completed (Snapshot In Progress)
    // =========================
    {{blob_file_gc_state::blob_scan_completed_snapshot_in_progress, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::cleaning_up},

    // =========================
    // Snapshot Scan Completed (Blob In Progress)
    // =========================
    {{blob_file_gc_state::snapshot_scan_completed_blob_in_progress, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::cleaning_up},

    // =========================
    // Cleaning Up
    // =========================
    {{blob_file_gc_state::cleaning_up, blob_file_gc_event::complete_cleanup}, blob_file_gc_state::completed},

    // =========================
    // Shutdown Transitions
    // =========================
    {{blob_file_gc_state::not_started, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::blob_scan_completed_snapshot_not_started, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::blob_scan_completed_snapshot_in_progress, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::snapshot_scan_completed_blob_not_started, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::snapshot_scan_completed_blob_in_progress, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::cleaning_up, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::completed, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},
    {{blob_file_gc_state::shutdown, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},

    // =========================
    // Reset Transition (After Shutdown)
    // =========================
    {{blob_file_gc_state::shutdown, blob_file_gc_event::reset}, blob_file_gc_state::not_started}
};



// ================= blob_file_gc_state_machine =================

/**
 * @brief Converts a state enum value to a human-readable string.
 */
std::string blob_file_gc_state_machine::to_string(blob_file_gc_state state) {
    static const std::unordered_map<blob_file_gc_state, std::string> state_strings = {
        {blob_file_gc_state::not_started, "Not Started"},
        {blob_file_gc_state::scanning_blob_only, "Scanning Blob Only"},
        {blob_file_gc_state::scanning_snapshot_only, "Scanning Snapshot Only"},
        {blob_file_gc_state::scanning_both, "Scanning Both"},
        {blob_file_gc_state::blob_scan_completed_snapshot_not_started, "Blob Scan Completed, Snapshot Not Started"},
        {blob_file_gc_state::blob_scan_completed_snapshot_in_progress, "Blob Scan Completed, Snapshot In Progress"},
        {blob_file_gc_state::snapshot_scan_completed_blob_not_started, "Snapshot Scan Completed, Blob Not Started"},
        {blob_file_gc_state::snapshot_scan_completed_blob_in_progress, "Snapshot Scan Completed, Blob In Progress"},
        {blob_file_gc_state::cleaning_up, "Cleaning Up"},
        {blob_file_gc_state::completed, "Completed"},
        {blob_file_gc_state::shutdown, "Shutdown"}
    };

    auto it = state_strings.find(state);
    return (it != state_strings.end()) ? it->second : "Unknown State";
}

blob_file_gc_state blob_file_gc_state_machine::transition(blob_file_gc_event event) {
    state_event_pair key = {current_state_, event};
    auto it = state_transition_map.find(key);
    if (it == state_transition_map.end()) {
        throw std::logic_error("Invalid transition: " + to_string(current_state_) + 
                               " with event " + to_string(event));
    }
    return it->second;
}

std::string blob_file_gc_state_machine::to_string(blob_file_gc_event event) {
    static const std::unordered_map<blob_file_gc_event, std::string> event_strings = {
        {blob_file_gc_event::start_blob_scan, "Start Blob Scan"},
        {blob_file_gc_event::start_snapshot_scan, "Start Snapshot Scan"},
        {blob_file_gc_event::complete_blob_scan, "Complete Blob Scan"},
        {blob_file_gc_event::complete_snapshot_scan, "Complete Snapshot Scan"},
        {blob_file_gc_event::complete_cleanup, "Complete Cleanup"},
        {blob_file_gc_event::shutdown, "Shutdown"},
        {blob_file_gc_event::reset, "Reset"}
    };

    auto it = event_strings.find(event);
    return (it != event_strings.end()) ? it->second : "Unknown Event";
}


std::optional<blob_file_gc_state> blob_file_gc_state_machine::get_next_state_if_valid(blob_file_gc_state current, blob_file_gc_event event) const {
    auto it = state_transition_map.find({current, event});
    if (it != state_transition_map.end()) {
        return it->second; // Valid transition, return next state
    }
    return std::nullopt; // Invalid transition
}

blob_file_gc_state blob_file_gc_state_machine::get_state() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_state_;
}

blob_file_gc_state blob_file_gc_state_machine::start_blob_scan() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ =  transition(blob_file_gc_event::start_blob_scan);
    return current_state_;
}

blob_file_gc_state blob_file_gc_state_machine::start_snapshot_scan(snapshot_scan_mode mode) {
    if (mode != snapshot_scan_mode::internal && mode != snapshot_scan_mode::external) {
        throw std::invalid_argument("Invalid snapshot scan mode");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ = transition(blob_file_gc_event::start_snapshot_scan);
    // If an exception is thrown when the state transition fails,
    // reaching this line indicates that the state transition was successful.
    snapshot_scan_mode_ = mode; 
    return current_state_; 
}

blob_file_gc_state blob_file_gc_state_machine::complete_blob_scan() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ = transition(blob_file_gc_event::complete_blob_scan);
    return current_state_;
}

blob_file_gc_state blob_file_gc_state_machine::complete_snapshot_scan(snapshot_scan_mode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    // If an exception is thrown when the state transition fails,
    // reaching this line indicates that the state transition was successful.
    blob_file_gc_state state = transition(blob_file_gc_event::complete_snapshot_scan);
    if (snapshot_scan_mode_ != mode) {
        throw std::logic_error("Snapshot scan mode mismatch");
    }
    snapshot_scan_mode_ = snapshot_scan_mode::none;
    current_state_ = state;
    return current_state_;
}

blob_file_gc_state blob_file_gc_state_machine::complete_cleanup() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ =  transition(blob_file_gc_event::complete_cleanup);
    return current_state_;
}

blob_file_gc_state blob_file_gc_state_machine::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ =  transition(blob_file_gc_event::shutdown);
    return current_state_;
}

blob_file_gc_state  blob_file_gc_state_machine::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ =  transition(blob_file_gc_event::reset);
    snapshot_scan_mode_ = snapshot_scan_mode::none;
    return current_state_;
}

void blob_file_gc_state_machine::force_set_state(blob_file_gc_state new_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ = new_state;
}

blob_file_gc_state_machine::snapshot_scan_mode blob_file_gc_state_machine::get_snapshot_scan_mode() {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_scan_mode_;
}    

 }  // namespace limestone::internal