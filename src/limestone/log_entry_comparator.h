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

 #include "limestone/api/write_version_type.h"
 #include "log_entry.h"
 
 namespace limestone::internal {
 
 using namespace limestone::api;

 class log_entry_comparator {
 public:
     /// @brief Compare log_entry objects in ascending order.
     ///
     /// First, compare key_sid() in lexicographical (ascending) order.
     /// If they differ, return the result of that comparison.
     /// If key_sid() values are equal, then compare write_version in ascending order.
     bool operator()(const log_entry& a, const log_entry& b) const {
         // Compare key_sid() in lexicographical order
         std::string_view key_a(a.key_sid());
         std::string_view key_b(b.key_sid());
         if (key_a != key_b) {
             return key_a < key_b;
         }
         // If key_sid() values are equal, compare write_version in ascending order
         write_version_type wa;
         write_version_type wb;
         a.write_version(wa);
         b.write_version(wb);
         return wa < wb;
     }
 };

 } // namespace limestone::internal
 



