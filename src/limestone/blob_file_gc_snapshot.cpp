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
 thread_local std::shared_ptr<log_entry_container> blob_file_gc_snapshot::tls_low_container_ = nullptr;
 // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
 thread_local std::shared_ptr<log_entry_container> blob_file_gc_snapshot::tls_high_container_ = nullptr;
 
 blob_file_gc_snapshot::blob_file_gc_snapshot(const write_version_type& boundary_version) : boundary_version_(boundary_version) {
     // The thread_low_containers_, thread_high_containers_ and snapshot_ are default-constructed.
 }
 
 blob_file_gc_snapshot::~blob_file_gc_snapshot() {
     // Reset the thread-local containers to avoid state leakage between tests or re-use in the same thread.
     tls_low_container_.reset();
     tls_high_container_.reset();
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
 
     // Dispatch entry to the appropriate container based on write_version.
     if (entry_wv < boundary_version_) {
         // Process low container.
         if (!tls_low_container_) {
             tls_low_container_ = std::make_shared<log_entry_container>();
             {
                 std::lock_guard<std::mutex> lock(global_mtx_);
                 thread_low_containers_.push_back(tls_low_container_);
             }
         }
         tls_low_container_->append(modified_entry);
     } else {
         // Process high container.
         if (!tls_high_container_) {
             tls_high_container_ = std::make_shared<log_entry_container>();
             {
                 std::lock_guard<std::mutex> lock(global_mtx_);
                 thread_high_containers_.push_back(tls_high_container_);
             }
         }
         tls_high_container_->append(modified_entry);
     }
 }
 
 void blob_file_gc_snapshot::finalize_local_entries() {
     if (tls_low_container_) {
         tls_low_container_->sort_descending();
         tls_low_container_.reset();
     }
     if (tls_high_container_) {
         // For high container, no sorting is required.
         tls_high_container_.reset();
     }
 }
 
 const log_entry_container& blob_file_gc_snapshot::finalize_snapshot() {
     // Process low containers: merge, sort, and remove duplicate entries.
     log_entry_container low_merged = log_entry_container::merge_sorted_collections(thread_low_containers_);
     aggregated_entries.clear();
     std::string last_key;
     for (const auto& entry : low_merged) {
         const std::string& current_key = entry.key_sid();
         if (last_key.empty() || current_key != last_key) {
             aggregated_entries.append(entry);
             last_key = current_key;
         }
     }
 
     // Process high containers: append all entries directly.
     for (const auto& high_container : thread_high_containers_) {
         for (const auto& entry : *high_container) {
             aggregated_entries.append(entry);
         }
     }
 
     return aggregated_entries;
 }
 
 void blob_file_gc_snapshot::reset() {
     {
         std::lock_guard<std::mutex> lock(global_mtx_);
         thread_low_containers_.clear();
         thread_high_containers_.clear();
     }
     aggregated_entries.clear();
     // Note: The thread_local containers remain set in each thread.
     // Their lifetime is managed per thread; if needed, threads can reset them.
 }
 
 const write_version_type& blob_file_gc_snapshot::boundary_version() const {
     return boundary_version_;
 }
 
 } // namespace limestone::internal
 