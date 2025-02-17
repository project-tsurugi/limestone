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

 #include "blob_file_gc_snapshot.h"
 #include "log_entry_container.h"
 #include "log_entry_comparator.h"
 #include <algorithm>
 
 namespace limestone::internal {
  

// ----------------- Implementation of blob_file_gc_snapshot methods -----------------
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local std::shared_ptr<log_entry_container> blob_file_gc_snapshot::tls_container_ = nullptr;

 blob_file_gc_snapshot::blob_file_gc_snapshot(const write_version_type& threshold)
     : threshold_(threshold) {
     // The thread_containers_ vector and snapshot_ are default-constructed.
 }

 blob_file_gc_snapshot::~blob_file_gc_snapshot() {
    // Reset the thread-local container to avoid state leakage between tests or re-use in the same thread.
    tls_container_.reset();
}


 void blob_file_gc_snapshot::sanitize_and_add_entry(const log_entry& entry) {
     // Only process entries of type normal_with_blob.
     if (entry.type() != log_entry::entry_type::normal_with_blob) {
         return;
     }

     // Create a modifiable copy of the entry.
     // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
     log_entry modified_entry = entry;

     // Truncate unnecessary data from the value_etc field.
     modified_entry.truncate_value_from_normal_entry();

     // Obtain the write_version from the modified entry.
     write_version_type entry_wv;
     modified_entry.write_version(entry_wv);

     // Compare the obtained write_version with the threshold.
     if (!(entry_wv < threshold_)) {
         return;
     }

     // Obtain or create the thread-local log_entry_container.
     if (!tls_container_) {
         tls_container_ = std::make_shared<log_entry_container>();
         {
             std::lock_guard<std::mutex> lock(global_mtx_);
             thread_containers_.push_back(tls_container_);
         }
     }

     // Append the modified entry into the thread-local container.
     tls_container_->append(modified_entry);
 }

 void blob_file_gc_snapshot::finalize_local_entries() {
    if (tls_container_) {
        tls_container_->sort_descending();
        tls_container_.reset();
    }
 }

 const log_entry_container& blob_file_gc_snapshot::finalize_snapshot() {
     log_entry_container merged = log_entry_container::merge_sorted_collections(thread_containers_);

     // Remove duplicate entries from the merged container.
     // Since the container is sorted in descending order, the first entry for a given key_sid
     // is the one with the maximum write_version.
     snapshot_.clear();
     std::string last_key;
     for (const auto& entry : merged) {
         const std::string& current_key = entry.key_sid();
         if (last_key.empty() || current_key != last_key) {
             snapshot_.append(entry);
             last_key = current_key;
         }
     }

     return snapshot_;
 }

 void blob_file_gc_snapshot::reset() {
     {
         // Clean up any remaining thread-local containers.
         std::lock_guard<std::mutex> lock(global_mtx_);
         thread_containers_.clear();
     }
     snapshot_.clear();
     // Note: The thread_local tls_container remains set in each thread.
     // Its lifetime is managed per thread; if needed, threads can reset it.
 }

 } // namespace limestone::internal
  