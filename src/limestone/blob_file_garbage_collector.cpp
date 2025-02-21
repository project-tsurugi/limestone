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

 namespace {
     class my_cursor : public limestone::internal::cursor_impl {
     public:
         using cursor_impl::cursor_impl;
         using cursor_impl::close;
         using cursor_impl::next;
         using cursor_impl::blob_ids;
         using cursor_impl::type;
     };
 }
 
 namespace std {
    template <>
    struct hash<std::pair<limestone::internal::blob_file_gc_state, limestone::internal::blob_file_gc_event>> {
        size_t operator()(const std::pair<limestone::internal::blob_file_gc_state, limestone::internal::blob_file_gc_event>& pair) const noexcept {
            return std::hash<int>()(static_cast<int>(pair.first)) ^ (std::hash<int>()(static_cast<int>(pair.second)) << 1);
        }
    };
}

 namespace limestone::internal {

    using limestone::api::log_entry;    


/**
 * @brief Defines a key for the state transition map.
 */
using state_event_pair = std::pair<blob_file_gc_state, blob_file_gc_event>;

static const std::unordered_map<state_event_pair, blob_file_gc_state> state_transition_map = {
    // Existing transitions...
    {{blob_file_gc_state::not_started, blob_file_gc_event::start_blob_scan}, blob_file_gc_state::scanning_blob_only},
    {{blob_file_gc_state::not_started, blob_file_gc_event::start_snapshot_scan}, blob_file_gc_state::scanning_snapshot_only},
    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::start_snapshot_scan}, blob_file_gc_state::scanning_both},
    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::start_blob_scan}, blob_file_gc_state::scanning_both},

    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::blob_scan_completed_snapshot_not_started},
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::blob_scan_completed_snapshot_in_progress},
    {{blob_file_gc_state::snapshot_scan_completed_blob_not_started, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::cleaning_up},
    {{blob_file_gc_state::snapshot_scan_completed_blob_in_progress, blob_file_gc_event::complete_blob_scan}, blob_file_gc_state::cleaning_up},

    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::snapshot_scan_completed_blob_not_started},
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::snapshot_scan_completed_blob_in_progress},
    {{blob_file_gc_state::blob_scan_completed_snapshot_not_started, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::cleaning_up},
    {{blob_file_gc_state::blob_scan_completed_snapshot_in_progress, blob_file_gc_event::complete_snapshot_scan}, blob_file_gc_state::cleaning_up},

    {{blob_file_gc_state::not_started, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::snapshot_scan_completed_blob_not_started},
    {{blob_file_gc_state::scanning_blob_only, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::snapshot_scan_completed_blob_not_started},
    {{blob_file_gc_state::scanning_snapshot_only, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::snapshot_scan_completed_blob_in_progress},
    {{blob_file_gc_state::scanning_both, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::snapshot_scan_completed_blob_in_progress},
    {{blob_file_gc_state::blob_scan_completed_snapshot_not_started, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::cleaning_up},
    {{blob_file_gc_state::blob_scan_completed_snapshot_in_progress, blob_file_gc_event::notify_snapshot_scan_progress}, blob_file_gc_state::cleaning_up},

    {{blob_file_gc_state::cleaning_up, blob_file_gc_event::complete_cleanup}, blob_file_gc_state::completed},

    // Shutdown event should be allowed from all states
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

    // Shutdown state should stay in shutdown
    {{blob_file_gc_state::shutdown, blob_file_gc_event::shutdown}, blob_file_gc_state::shutdown},

    // Reset event to restart after shutdown
    {{blob_file_gc_state::shutdown, blob_file_gc_event::reset}, blob_file_gc_state::not_started}
};


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
    std::lock_guard<std::mutex> lock(mutex_);

    state_event_pair key = {current_state_, event};

    auto it = state_transition_map.find(key);
    if (it == state_transition_map.end()) {
        throw std::logic_error("Invalid transition: " + to_string(current_state_) + 
                               " with event " + std::to_string(static_cast<int>(event)));
    }

    current_state_ = it->second;
    return current_state_;
}

std::string blob_file_gc_state_machine::to_string(blob_file_gc_event event) {
    static const std::unordered_map<blob_file_gc_event, std::string> event_strings = {
        {blob_file_gc_event::start_blob_scan, "Start Blob Scan"},
        {blob_file_gc_event::start_snapshot_scan, "Start Snapshot Scan"},
        {blob_file_gc_event::complete_blob_scan, "Complete Blob Scan"},
        {blob_file_gc_event::complete_snapshot_scan, "Complete Snapshot Scan"},
        {blob_file_gc_event::notify_snapshot_scan_progress, "Notify Snapshot Scan Progress"},
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

blob_file_gc_state blob_file_gc_state_machine::start_blob_scan() {
    return transition(blob_file_gc_event::start_blob_scan);
}

blob_file_gc_state blob_file_gc_state_machine::start_snapshot_scan() {
    return transition(blob_file_gc_event::start_snapshot_scan);
}

blob_file_gc_state blob_file_gc_state_machine::complete_blob_scan() {
    return transition(blob_file_gc_event::complete_blob_scan);
}

blob_file_gc_state blob_file_gc_state_machine::complete_snapshot_scan() {
    return transition(blob_file_gc_event::complete_snapshot_scan);
}

blob_file_gc_state blob_file_gc_state_machine::notify_snapshot_scan_progress() {
    return transition(blob_file_gc_event::notify_snapshot_scan_progress);
}

blob_file_gc_state blob_file_gc_state_machine::complete_cleanup() {
    return transition(blob_file_gc_event::complete_cleanup);
}

blob_file_gc_state blob_file_gc_state_machine::shutdown() {
    return transition(blob_file_gc_event::shutdown);
}

void blob_file_gc_state_machine::force_set_state(blob_file_gc_state new_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    current_state_ = new_state;
}
  
 // Constructor now takes a blob_file_resolver and sets the resolver_ member.
 blob_file_garbage_collector::blob_file_garbage_collector(const blob_file_resolver& resolver)
     : resolver_(&resolver),
       scanned_blobs_(std::make_unique<blob_id_container>()),
       gc_exempt_blob_(std::make_unique<blob_id_container>()) {
     file_ops_ = std::make_unique<real_file_operations>(); 
 }

 blob_file_garbage_collector::~blob_file_garbage_collector() {
     try {
         shutdown();
     } catch (const std::exception &e) {
         LOG_LP(ERROR) << "Exception in destructor during shutdown: " << e.what();
     } catch (...) {
         LOG_LP(ERROR) << "Unknown exception in destructor during shutdown.";
     }
 }

 void blob_file_garbage_collector::scan_blob_files(blob_id_type max_existing_blob_id) {
     {
         std::lock_guard<std::mutex> lock(mutex_);
         if (blob_file_scan_waited_) {
             throw std::logic_error("Cannot start scan after wait_for_blob_file_scan() has been called.");
         }
         if (blob_file_scan_started_) {
             throw std::logic_error("Scan has already been started.");
         }
         blob_file_scan_started_ = true;
         max_existing_blob_id_ = max_existing_blob_id;
         blob_file_scan_complete_ = false;
     }
     // Launch the scanning thread
     blob_file_scan_thread_ = std::thread(&blob_file_garbage_collector::scan_directory, this);
 }
  
 void blob_file_garbage_collector::scan_directory() {
     pthread_setname_np(pthread_self(), "lstone_scan_blb");
     try {
         // Obtain the root directory from the resolver.
         boost::filesystem::path root = resolver_->get_blob_root();
  
         // Iterate recursively over the root directory.
         for (boost::filesystem::recursive_directory_iterator it(root), end; it != end; ++it) {
            if (shutdown_requested_.load(std::memory_order_acquire)) {
                break;
            }
            if (boost::filesystem::is_regular_file(it->path())) {
                 const boost::filesystem::path& file_path = it->path();
  
                 // Use blob_file_resolver's function to check if this file is a valid blob file.
                 if (!resolver_->is_blob_file(file_path)) {
                     continue;
                 }
  
                 blob_id_type id = resolver_->extract_blob_id(file_path);
                 VLOG_LP(log_trace_fine) << "Scanned blob file: " << file_path.string();
                 if (id <= max_existing_blob_id_) {
                     scanned_blobs_->add_blob_id(id);
                     VLOG_LP(log_trace_fine) << "Added blob id: " << id;
                 }
             }
         }
         VLOG_LP(log_trace_fine) << "Blob file scan complete.";
     } catch (const std::exception &e) {
         LOG_LP(ERROR) << "Exception in blob_file_garbage_collector::scan_directory: " << e.what();
     }
     {
         std::lock_guard<std::mutex> lock(mutex_);
         blob_file_scan_complete_ = true;
     }
     blob_file_scan_cv_.notify_all();
 }
  
 void blob_file_garbage_collector::add_gc_exempt_blob_id(blob_id_type id) {
    VLOG_LP(log_trace_fine) << "Adding blob id to gc_exempt_blob_: " << id;
     gc_exempt_blob_->add_blob_id(id);
 }
  
 void blob_file_garbage_collector::finalize_scan_and_cleanup() {
     {
         std::lock_guard<std::mutex> lock(mutex_);
         if (cleanup_waited_) {
             throw std::logic_error("Cannot start cleanup after wait_for_cleanup() has been called.");
         }
         // Mark the start of the cleanup process
         cleanup_started_ = true;
     }
     cleanup_thread_ = std::thread([this]() {
         pthread_setname_np(pthread_self(), "lstone_cleanup");
  
         // Wait for the scan to complete
         this->wait_for_blob_file_scan();
          
         // Calculate the difference and perform deletion operations
         VLOG_LP(100) << "Scanned blobs before diff: " << scanned_blobs_->debug_string();
         VLOG_LP(100) << "GC exempt blobs: " << gc_exempt_blob_->debug_string();
         scanned_blobs_->diff(*gc_exempt_blob_);
         VLOG_LP(100) << "Scanned blobs after: " << scanned_blobs_->debug_string();

         for (const auto &id : *scanned_blobs_) {
            if (shutdown_requested_.load(std::memory_order_acquire)) {
                break;
            }
             boost::filesystem::path file_path = resolver_->resolve_path(id);
             boost::system::error_code ec;
             VLOG_LP(log_trace_fine) << "Removing blob id: " << id;
             VLOG_LP(log_trace_fine) << "Removing blob file: " << file_path.string();
             file_ops_->remove(file_path, ec);
             if (ec && ec != boost::system::errc::no_such_file_or_directory) {
                 LOG_LP(ERROR) << "Failed to remove file: " << file_path.string()
                               << " Error: " << ec.message();
             }
         }
         {
             std::lock_guard<std::mutex> lock(mutex_);
             // Mark that cleanup is complete
             cleanup_complete_ = true;
         }
         // Notify any thread waiting for cleanup completion
         cleanup_cv_.notify_all();
         reset();
     });
 }
  
 void blob_file_garbage_collector::wait_for_blob_file_scan() {
     std::unique_lock<std::mutex> lock(mutex_);
     // Mark that wait_for_blob_file_scan() has been called.
     blob_file_scan_waited_ = true;
     // If the scan has not been started, return immediately.
     if (!blob_file_scan_started_) {
         return;
     }
     // Wait until the scan is complete.
     blob_file_scan_cv_.wait(lock, [this]() { return blob_file_scan_complete_; });
 }
  
 void blob_file_garbage_collector::wait_for_cleanup() {
     std::unique_lock<std::mutex> lock(mutex_);
     // Mark that wait_for_cleanup() has been called.
     cleanup_waited_ = true;
     // If cleanup has not started, return immediately to avoid indefinite blocking.
     if (!cleanup_started_) {
         return;
     }
     // Wait until the cleanup process is complete.
     cleanup_cv_.wait(lock, [this]() { return cleanup_complete_; });
 }
  
 void blob_file_garbage_collector::set_file_operations(std::unique_ptr<file_operations> file_ops) {
     file_ops_ = std::move(file_ops);
 }
  
 void blob_file_garbage_collector::shutdown() {
    // Use a dedicated mutex to ensure shutdown() is executed exclusively.
    std::lock_guard<std::mutex> shutdown_lock(shutdown_mutex_);

    shutdown_requested_.store(true, std::memory_order_release);

    wait_for_all_threads();

     shutdown_requested_.store(false, std::memory_order_release);
 }
  

void blob_file_garbage_collector::wait_for_all_threads() {
    wait_for_blob_file_scan();
    wait_for_scan_snapshot();
    wait_for_cleanup();

    if (blob_file_scan_thread_.joinable()) {
        blob_file_scan_thread_.join();
    }
    if (snapshot_scan_thread_.joinable()) {
        snapshot_scan_thread_.join();
    }
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    reset();
}



void blob_file_garbage_collector::scan_snapshot(const boost::filesystem::path &snapshot_file, const boost::filesystem::path &compacted_file) {
    std::unique_ptr<my_cursor> cur;
    if (boost::filesystem::exists(compacted_file)) {
        cur = std::make_unique<my_cursor>(snapshot_file, compacted_file);
    } else {
        cur = std::make_unique<my_cursor>(snapshot_file);
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (snapshot_scan_started_) {
            throw std::logic_error("Snapshot scanning has already been started.");
        }
        snapshot_scan_started_ = true;
        snapshot_scan_complete_ = false;
    }
    
    // Launch the snapshot scanning thread with the pre-created cursor.
    snapshot_scan_thread_ = std::thread([this, cur = std::move(cur)]() {
        try {
            while (cur->next()) {
                if (shutdown_requested_.load(std::memory_order_acquire)) {
                    break;
                }
                if (cur->type() == log_entry::entry_type::normal_with_blob) {
                    auto blob_ids = cur->blob_ids();
                    for (auto id : blob_ids) {
                        VLOG_LP(log_trace_fine) << "Scanned blob id: " << id;
                        gc_exempt_blob_->add_blob_id(id);
                    }
                }
            }
            VLOG_LP(log_trace_fine) << "Snapshot scan finished.";
            finalize_scan_and_cleanup();
        } catch (const limestone_exception &e) {
            LOG_LP(ERROR) << "Exception in snapshot scan thread: " << e.what();
        } catch (const std::exception &e) {
            LOG_LP(ERROR) << "Standard exception in snapshot scan thread: " << e.what();
        } catch (...) {
            LOG_LP(ERROR) << "Unknown exception in snapshot scan thread.";
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            snapshot_scan_complete_ = true;
        }
        snapshot_scan_cv_.notify_all();
    });
}
  
 void blob_file_garbage_collector::wait_for_scan_snapshot() {
     std::unique_lock<std::mutex> lock(mutex_);
     snapshot_scan_waited_ = true;
     if (!snapshot_scan_started_) {
         return;
     }
     snapshot_scan_cv_.wait(lock, [this]() { return snapshot_scan_complete_; });
 }
  
 void blob_file_garbage_collector::reset() {
     std::lock_guard<std::mutex> lock(mutex_);
     scanned_blobs_ = std::make_unique<blob_id_container>();
     gc_exempt_blob_ = std::make_unique<blob_id_container>();
     blob_file_scan_started_ = false;
     blob_file_scan_waited_ = false;
     snapshot_scan_started_ = false;
     snapshot_scan_waited_ = false;
     cleanup_started_ = false;
     cleanup_waited_ = false;
 }
 
 bool blob_file_garbage_collector::is_active() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !(blob_file_scan_complete_ && snapshot_scan_complete_ && cleanup_complete_);
}

 } // namespace limestone::internal
 