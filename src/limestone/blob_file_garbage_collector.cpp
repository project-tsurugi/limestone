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
 #include "blob_file_scanner.h"
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
 
 

 namespace limestone::internal {

    using limestone::api::log_entry;    
  
 // Constructor now takes a blob_file_resolver and sets the resolver_ member.
 blob_file_garbage_collector::blob_file_garbage_collector(const blob_file_resolver& resolver)
     : resolver_(&resolver),
       scanned_blobs_(std::make_unique<blob_id_container>()),
       gc_exempt_blob_(std::make_unique<blob_id_container>()) {
     file_ops_ = std::make_unique<real_file_operations>();
     state_machine_.force_set_state(blob_file_gc_state::not_started); 
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
     state_machine_.start_blob_scan();
     max_existing_blob_id_ = max_existing_blob_id;
     blob_file_scan_thread_ = std::thread(&blob_file_garbage_collector::scan_directory, this);
 }

 void blob_file_garbage_collector::scan_directory() {
    pthread_setname_np(pthread_self(), "lstone_scan_blb");
    try {
        // Initialize blob_file_scanner with the resolver
        blob_file_scanner scanner(resolver_);

        // Iterate over each blob file using the scanner
        for (const auto& file_path : scanner) {
            if (shutdown_requested_.load(std::memory_order_acquire)) {
                break;
            }

            blob_id_type id = resolver_->extract_blob_id(file_path);
            VLOG_LP(log_trace) << "Scanned blob file: " << file_path.string();
            if (id <= max_existing_blob_id_) {
                scanned_blobs_->add_blob_id(id);
                VLOG_LP(log_trace) << "Added blob id: " << id;
            }
        }
        VLOG_LP(log_trace) << "Blob file scan complete.";
    } catch (const std::exception &e) {
        LOG_LP(ERROR) << "Exception in blob_file_garbage_collector::scan_directory: " << e.what();
    }
    state_machine_.complete_blob_scan();
    blob_file_scan_cv_.notify_all();
}
  
 void blob_file_garbage_collector::add_gc_exempt_blob_id(blob_id_type id) {
    VLOG_LP(log_trace) << "Adding blob id to gc_exempt_blob_: " << id;
    if (state_machine_.get_snapshot_scan_mode() != blob_file_gc_state_machine::snapshot_scan_mode::external) {
        throw std::logic_error("Cannot add GC exempt blob id before starting the scan.");
    }
     gc_exempt_blob_->add_blob_id(id);
 }
  
 void blob_file_garbage_collector::finalize_scan_and_cleanup() {
     cleanup_thread_ = std::thread([this]() {
         pthread_setname_np(pthread_self(), "lstone_cleanup");
  
         // Wait for the scan to complete
         this->wait_for_blob_file_scan();
          
         // Calculate the difference and perform deletion operations
         VLOG_LP(log_trace_fine) << "Scanned blobs before diff: " << scanned_blobs_->debug_string();
         VLOG_LP(log_trace_fine) << "GC exempt blobs: " << gc_exempt_blob_->debug_string();
         scanned_blobs_->diff(*gc_exempt_blob_);
         VLOG_LP(log_trace_fine) << "Scanned blobs after: " << scanned_blobs_->debug_string();

         for (const auto &id : *scanned_blobs_) {
            if (shutdown_requested_.load(std::memory_order_acquire)) {
                break;
            }
             boost::filesystem::path file_path = resolver_->resolve_path(id);
             boost::system::error_code ec;
             VLOG_LP(log_trace) << "Removing blob id: " << id;
             VLOG_LP(log_trace) << "Removing blob file: " << file_path.string();
             file_ops_->remove(file_path, ec);
             if (ec && ec != boost::system::errc::no_such_file_or_directory) {
                 LOG_LP(ERROR) << "Failed to remove file: " << file_path.string()
                               << " Error: " << ec.message();
             }
         }
         state_machine_.complete_cleanup();
         VLOG_LP(log_trace) << "Notifying cleanup_cv_";
         {
            std::lock_guard<std::mutex> lock(mutex_);
            cleanup_cv_.notify_all();
        }
     });
 }
  
void blob_file_garbage_collector::wait_for_blob_file_scan() {
    VLOG_LP(log_trace) << "entering wait_for_blob_file_scan";
    // If the scan has not been started, return immediately.
    blob_file_gc_state current_state = state_machine_.get_state();
    if (current_state != blob_file_gc_state::scanning_blob_only &&
        current_state != blob_file_gc_state::scanning_both &&
        current_state != blob_file_gc_state::snapshot_scan_completed_blob_not_started &&
        current_state != blob_file_gc_state::snapshot_scan_completed_blob_in_progress) {
        VLOG_LP(log_trace) << "wait_for_blob_file_scan returning immediately";
        return;
    }

    // Wait until the scan is complete or shutdown is requested
    std::unique_lock<std::mutex> lock(mutex_);
    blob_file_scan_cv_.wait(lock, [this]() {
        blob_file_gc_state state = state_machine_.get_state();
        return state == blob_file_gc_state::blob_scan_completed_snapshot_not_started ||
               state == blob_file_gc_state::blob_scan_completed_snapshot_in_progress ||
               state == blob_file_gc_state::cleaning_up ||
               state == blob_file_gc_state::completed ||
               shutdown_requested_.load(std::memory_order_acquire);
    });

    VLOG_LP(log_trace) << "Exiting wait_for_blob_file_scan";
}
   

  
void blob_file_garbage_collector::wait_for_cleanup() {
    // If cleanup has not started, return immediately to avoid indefinite blocking.
    VLOG_LP(log_trace) << "entering wait_for_cleanup";
    blob_file_gc_state current_state = state_machine_.get_state();
    if (current_state == blob_file_gc_state::completed|| 
        current_state == blob_file_gc_state::not_started ||
        current_state == blob_file_gc_state::shutdown) {
        VLOG_LP(log_trace) << "wait_for_cleanup returning immediately.";
        return;
    }

    // Wait until the cleanup process is complete.
    VLOG_LP(log_trace) << "Waiting for cleanup_cv_";
    std::unique_lock<std::mutex> lock(mutex_);
    cleanup_cv_.wait(lock, [this]() {
        ;
        blob_file_gc_state state = state_machine_.get_state();
        return state == blob_file_gc_state::completed || shutdown_requested_.load(std::memory_order_acquire);
    });
}


 void blob_file_garbage_collector::set_file_operations(std::unique_ptr<file_operations> file_ops) {
     file_ops_ = std::move(file_ops);
 }

 void blob_file_garbage_collector::shutdown() {
     VLOG_LP(log_trace) << "Calling shutdown...";
     {
         std::lock_guard<std::mutex> lock(shutdown_mutex_);

         // Set shutdown flag before notifying waiting threads
         state_machine_.shutdown();

         // Set the shutdown request.
         shutdown_requested_.store(true, std::memory_order_release);

         // Wake up all threads that are waiting in their respective wait() calls.
         {
             std::lock_guard<std::mutex> lock(mutex_);
             blob_file_scan_cv_.notify_all();
             snapshot_scan_cv_.notify_all();
             cleanup_cv_.notify_all();
         }

         // Ensure that all threads can join before they enter a wait().
         wait_for_all_threads();
         shutdown_requested_.store(false, std::memory_order_release);
         reset();
     }
     VLOG_LP(log_trace) << "Shutdown complete.";
 }

void blob_file_garbage_collector::wait_for_all_threads() {
    if (blob_file_scan_thread_.joinable()) {
        blob_file_scan_thread_.join();
    }
    if (snapshot_scan_thread_.joinable()) {
        snapshot_scan_thread_.join();
    }
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

void blob_file_garbage_collector::scan_snapshot(const boost::filesystem::path &snapshot_file, const boost::filesystem::path &compacted_file) {
    state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
    std::unique_ptr<my_cursor> cur;
    if (boost::filesystem::exists(compacted_file)) {
        cur = std::make_unique<my_cursor>(snapshot_file, compacted_file);
    } else {
        cur = std::make_unique<my_cursor>(snapshot_file);
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
                        VLOG_LP(log_trace) << "Scanned blob id: " << id;
                        gc_exempt_blob_->add_blob_id(id);
                    }
                }
            }
            VLOG_LP(log_trace) << "Snapshot scan finished.";
            state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
            finalize_scan_and_cleanup();
        } catch (const limestone_exception &e) {
            LOG_LP(ERROR) << "Exception in snapshot scan thread: " << e.what();
        } catch (const std::exception &e) {
            LOG_LP(ERROR) << "Standard exception in snapshot scan thread: " << e.what();
        } catch (...) {
            LOG_LP(ERROR) << "Unknown exception in snapshot scan thread.";
        }
        snapshot_scan_cv_.notify_all();
    });
}

void blob_file_garbage_collector::start_add_gc_exempt_blob_ids() {
    state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
}

void blob_file_garbage_collector::finalize_add_gc_exempt_blob_ids() {
    state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
    finalize_scan_and_cleanup(); 
    snapshot_scan_cv_.notify_all();
}
  
void blob_file_garbage_collector::wait_for_scan_snapshot() {
    VLOG_LP(log_trace) << "entering wait_for_scan_snapshot";
    // If the snapshot scan has not started, return immediately.
    blob_file_gc_state current_state = state_machine_.get_state();
    if (current_state != blob_file_gc_state::scanning_snapshot_only &&
        current_state != blob_file_gc_state::scanning_both &&
        current_state != blob_file_gc_state::blob_scan_completed_snapshot_not_started &&
        current_state != blob_file_gc_state::blob_scan_completed_snapshot_in_progress) {
        VLOG_LP(log_trace) << "wait_for_scan_snapshot returning immediately";
        return;
    }

    // Wait until the snapshot scan is complete.
    VLOG_LP(log_trace) << "Waiting for snapshot_scan_cv_";
    std::unique_lock<std::mutex> lock(mutex_);
    snapshot_scan_cv_.wait(lock, [this]() {
        blob_file_gc_state state = state_machine_.get_state();
        return state == blob_file_gc_state::snapshot_scan_completed_blob_not_started ||
               state == blob_file_gc_state::snapshot_scan_completed_blob_in_progress ||
               state == blob_file_gc_state::cleaning_up ||
               state == blob_file_gc_state::completed ||
               shutdown_requested_.load(std::memory_order_acquire);
    });

    VLOG_LP(log_trace) << "Exiting wait_for_scan_snapshot";
}


  
 void blob_file_garbage_collector::reset() {
    state_machine_.reset();
     scanned_blobs_ = std::make_unique<blob_id_container>();
     gc_exempt_blob_ = std::make_unique<blob_id_container>();
     max_existing_blob_id_ = 0;
 }

 bool blob_file_garbage_collector::is_active() const {
     blob_file_gc_state current_state = state_machine_.get_state();
     return current_state != blob_file_gc_state::not_started &&
            current_state != blob_file_gc_state::completed;
 }

 } // namespace limestone::internal
 