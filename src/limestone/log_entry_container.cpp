/*
 * Copyright 2022-2024 Project Tsurugi.
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

 #include "log_entry_container.h"
 #include "log_entry_comparator.h"
 #include <boost/heap/binomial_heap.hpp>
  #include <algorithm>   // for std::sort
 #include <iterator>    // for std::back_inserter
 
 namespace limestone::internal {
 
 // ----- Log Entry Addition (append) -----
 
 void log_entry_container::append(const log_entry& entry) {
     entries_.push_back(entry);
     // When a new entry is added, the collection is no longer sorted.
     sorted_ = false;
 }
 
 // ----- Iteration Functions -----
 
 log_entry_container::const_iterator log_entry_container::begin() const {
     return entries_.begin();
 }
 
 log_entry_container::const_iterator log_entry_container::end() const {
     return entries_.end();
 }
 
 std::size_t log_entry_container::size() const noexcept {
     return entries_.size();
 }
 
 void log_entry_container::clear() noexcept {
     entries_.clear();
     // An empty collection is considered sorted.
     sorted_ = true;
 }
 
 // ----- Sorting Functions -----
 
 void log_entry_container::sort() {
    // Only sort if the collection is not already sorted.
    if (!sorted_) {
        // Sort using log_entry_comparator.
        std::sort(entries_.begin(), entries_.end(), log_entry_comparator());
        sorted_ = true;
    }
}
 
 bool log_entry_container::is_sorted() const noexcept {
     return sorted_;
 }
 
 // ----- Static Merge Sort Function -----
 
 // Structure to hold the current iterator and the end iterator for a collection.
 struct iterator_range {
     std::vector<log_entry>::const_iterator current;
     std::vector<log_entry>::const_iterator end;
 };
 
 // Comparator for the heap: ensures that the largest key_sid is at the top.
 struct iterator_range_compare {
    bool operator()(const iterator_range& a, const iterator_range& b) const {
        return log_entry_comparator()(*a.current, *b.current);
    }
};

 
 // Static merge function using a multi-way merge algorithm with Boost's binomial_heap.
 log_entry_container log_entry_container::merge_sorted_collections(
     std::vector<log_entry_container>& container_list) {
 
     log_entry_container merged;
 
     // First, ensure that each collection is sorted.
     for (auto& container : container_list) {
         container.sort(); // This will ensure that each collection is sorted.
     }
 
     // Reserve capacity for the merged entries.
     std::size_t total_size = 0;
     for (const auto& container : container_list) {
         total_size += container.size();
     }
     // If total_size is 0, then all collections are empty; return an empty merged collection.
     if (total_size == 0) {
         return merged;
     }
 
     merged.entries_.reserve(total_size);
 
     // Define a Boost binomial_heap for iterator_range with our custom comparator.
     boost::heap::binomial_heap<iterator_range, boost::heap::compare<iterator_range_compare>> heap;
 
     // Push non-empty collections into the heap.
     for (auto& container : container_list) {
         if (container.size() > 0) {
             iterator_range range { container.begin(), container.end() };
             heap.push(range);
         }
     }
 
     // Multi-way merge: repeatedly extract the largest key_sid element and push the next element from that range.
     while (!heap.empty()) {
         auto top = heap.top();
         heap.pop();
 
         // Append the largest key_sid log_entry to the merged collection.
         merged.entries_.push_back(*(top.current));
 
         // Advance the iterator in the extracted range.
         auto next_it = top.current;
         ++next_it;
         if (next_it != top.end) {
             // If there are remaining elements in this range, push the updated range back into the heap.
             heap.push(iterator_range{ next_it, top.end });
         }
     }
 
     // Mark the merged collection as sorted.
     merged.sorted_ = true;
 
     // Clear the input collections as specified.
     for (auto& container : container_list) {
         container.clear();
     }
 
     return merged;
 }
 
 } // namespace limestone::internal
 