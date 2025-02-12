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

 #pragma once

 #include <vector>
 #include <cstddef>
 #include "log_entry.h"
 
 namespace limestone::internal {
 
 using namespace limestone::api;
 
 class log_entry_container {
     public:
         // ----- Log Entry Addition (append) -----
     
         /// @brief Adds a single log entry to the container.
         /// @param entry The log entry to add (by copy).
         ///
         /// Note: When a new entry is added, the internal sorted flag is set to false.
         void append(const log_entry& entry);
     
         // ----- Iteration Functions -----
     
         // Only expose const iterators for external iteration.
         using const_iterator = std::vector<log_entry>::const_iterator;
     
         /// @brief Returns a const iterator to the beginning of the container.
         const_iterator begin() const;
     
         /// @brief Returns a const iterator to the end of the container.
         const_iterator end() const;
     
         /// @brief Returns the number of log entries in the container.
         std::size_t size() const noexcept;
     
         /// @brief Clears all contents of the container.
         void clear() noexcept;
     
         // ----- Sorting Functions -----
     
         /// @brief Sorts the log entries using built-in sorting logic.
         ///
         /// The entries are sorted in descending order based on each log_entry's key_sid().
         /// After sorting is completed, the internal sorted flag is set to true.
         void sort();
     
         /// @brief Checks whether the container is currently sorted.
         /// @return true if the container is sorted; false otherwise.
         bool is_sorted() const noexcept;
     
         // ----- Static Merge Sort Function -----
     
         /// @brief Merges multiple sorted log_entry_container objects into a single sorted container.
         ///
         /// The merge is performed internally using a merge sort algorithm.
         /// The containers provided as arguments are merged, and the original containers are cleared.
         ///
         /// @param containers A vector of log_entry_container objects to merge.
         /// @return A single sorted log_entry_container containing all merged entries.
         static log_entry_container merge_sorted_collections(
             std::vector<log_entry_container>& containers);
     
     private:
         // Internal data structure (can be replaced later for optimizations like lazy loading)
         std::vector<log_entry> entries_;
     
         // Internal flag: indicates whether the container is currently sorted.
         bool sorted_ = true;
     };
 }
 
 
 