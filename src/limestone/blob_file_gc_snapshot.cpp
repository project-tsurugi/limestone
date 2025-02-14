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
 
 namespace limestone {
 namespace internal {

 
 // Thread-local pointer to each thread's log_entry_container.
 thread_local log_entry_container* tls_container = nullptr;

 
// ----------------- Implementation of blob_id_iterator -----------------

blob_id_iterator::blob_id_iterator(const log_entry_container* snapshot)
    : snapshot_(snapshot), entry_index_(0), blob_index_(0)
{
    // Ensure cache is initially empty.
    cached_blob_ids_.clear();
    advance_to_valid();
}

bool blob_id_iterator::has_next() const
{
    return (snapshot_ && entry_index_ < snapshot_->size());
}

limestone::api::blob_id_type blob_id_iterator::current()
{
    // Assume caller has checked has_next()
    // Use the cached blob IDs.
    limestone::api::blob_id_type result = cached_blob_ids_[blob_index_];
    ++blob_index_;
    advance_to_valid();
    return result;
}

void blob_id_iterator::advance_to_valid()
{
    // Loop until a valid blob ID is found or we run out of entries.
    while (snapshot_ && entry_index_ < snapshot_->size()) {
        // If the cache is empty, load the blob IDs from the current entry.
        if (cached_blob_ids_.empty()) {
            const auto& entry = *std::next(snapshot_->begin(), entry_index_);
            cached_blob_ids_ = entry.get_blob_ids();
        }
        if (blob_index_ < cached_blob_ids_.size()) {
            // Valid blob found.
            return;
        }
        // Otherwise, move to the next entry.
        ++entry_index_;
        blob_index_ = 0;
        cached_blob_ids_.clear(); // Clear cache so it will be reloaded for the new entry.
    }
    // If we exit the loop, no more valid blob IDs exist.
}


// ----------------- Implementation of blob_file_gc_snapshot methods -----------------


 blob_file_gc_snapshot::blob_file_gc_snapshot(const write_version_type& threshold)
     : threshold_(threshold)
 {
     // The thread_containers_ vector and snapshot_ are default-constructed.
 }
 
 void blob_file_gc_snapshot::sanitize_and_add_entry(const log_entry& entry)
 {
     // Only process entries of type normal_with_blob.
     if (entry.type() != log_entry::entry_type::normal_with_blob) {
         return;
     }
 
     // Create a modifiable copy of the entry.
     log_entry modified_entry = entry;
 
     // Clear the payload part of value_etc while keeping the header (write_version) intact.
     // The header size is determined by the logical sizes of epoch_id_type and std::uint64_t.
     constexpr std::size_t header_size = sizeof(limestone::api::epoch_id_type) + sizeof(std::uint64_t);
     std::string& mutable_value = const_cast<std::string&>(modified_entry.value_etc());
     if (mutable_value.size() > header_size) {
         mutable_value.resize(header_size);
     }
     
     // Obtain the write_version from the modified entry.
     write_version_type entry_wv;
     modified_entry.write_version(entry_wv);
     
     // Compare the obtained write_version with the threshold.
     if (!(entry_wv < threshold_)) {
         return;
     }
     
     // Obtain or create the thread-local log_entry_container.
     if (!tls_container) {
         tls_container = new log_entry_container();
         {
             std::lock_guard<std::mutex> lock(global_mtx_);
             thread_containers_.push_back(tls_container);
         }
     }
     
     // Append the modified entry into the thread-local container.
     tls_container->append(modified_entry);
 }
 
 void blob_file_gc_snapshot::finalize_local_entries()
{
    tls_container->sort_descending();
}
 
void blob_file_gc_snapshot::finalize_snapshot()
{
    // Prepare a vector to collect containers from all threads.
    std::vector<log_entry_container> containers_to_merge;
    {
        // Lock the global mutex to safely access thread_containers_.
        std::lock_guard<std::mutex> lock(global_mtx_);
        for (auto* container_ptr : thread_containers_) {
            // Although each thread should have already finalized its local container,
            // we call sort_descending() here as a defensive measure.
            container_ptr->sort_descending();
            // Move the container's content into the merging vector.
            containers_to_merge.push_back(std::move(*container_ptr));
            // Free the allocated memory.
            delete container_ptr;
        }
        // Clear the global container list.
        thread_containers_.clear();
    }
    
    // Merge all thread-local containers into a single container.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers_to_merge);
    
    // Remove duplicates directly into snapshot_.
    snapshot_.clear();
    // last_key is initialized to an empty string.
    // It is guaranteed that key_sid is never an empty string, so the first entry will always be appended.
    std::string last_key;
    for (auto it = merged.begin(); it != merged.end(); ++it) {
        const std::string& current_key = it->key_sid();
        if (last_key.empty() || current_key != last_key) {
            snapshot_.append(*it);
            last_key = current_key;
        }
    }
}

 
blob_id_iterator blob_file_gc_snapshot::blob_ids_iterator() const {
    return blob_id_iterator(&snapshot_);
}

 void blob_file_gc_snapshot::reset()
 {
     {
         // Clean up any remaining thread-local containers.
         std::lock_guard<std::mutex> lock(global_mtx_);
         for (auto* container_ptr : thread_containers_) {
             delete container_ptr;
         }
         thread_containers_.clear();
     }
     snapshot_.clear();
     // Note: The thread_local tls_container remains set in each thread.
     // Its lifetime is managed per thread; if needed, threads can reset it.
 }
 
 // These helper functions are no longer used because merging and duplicate removal are handled in finalize_snapshot.
 void blob_file_gc_snapshot::merge_entries() { }
 void blob_file_gc_snapshot::remove_duplicate_entries() { }
 
 } // namespace internal
 } // namespace limestone
 