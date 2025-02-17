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
             if (boost::filesystem::is_regular_file(it->path())) {
                 const boost::filesystem::path& file_path = it->path();
  
                 // Use blob_file_resolver's function to check if this file is a valid blob file.
                 if (!resolver_->is_blob_file(file_path)) {
                     continue;
                 }
  
                 blob_id_type id = resolver_->extract_blob_id(file_path);
                 if (id <= max_existing_blob_id_) {
                     scanned_blobs_->add_blob_id(id);
                 }
             }
         }
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
         scanned_blobs_->diff(*gc_exempt_blob_);
         for (const auto &id : *scanned_blobs_) {
             boost::filesystem::path file_path = resolver_->resolve_path(id);
             boost::system::error_code ec;
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
     {
         std::lock_guard<std::mutex> lock(mutex_);
         if (snapshot_scan_started_) {
             throw std::logic_error("Snapshot scanning has already been started.");
         }
         snapshot_scan_started_ = true;
         snapshot_scan_complete_ = false;
     }
 
     snapshot_scan_thread_ = std::thread([this, snapshot_file, compacted_file]() {
         try {
             auto cur = std::make_unique<my_cursor>(snapshot_file, compacted_file);
             while (cur->next()) {
                 if (cur->type() == log_entry::entry_type::normal_with_blob) {
                     auto blob_ids = cur->blob_ids();
                     for (auto id : blob_ids) {
                         gc_exempt_blob_->add_blob_id(id);
                     }
                 }
             }
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
 }
 
 } // namespace limestone::internal
 