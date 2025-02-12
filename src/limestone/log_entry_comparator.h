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
     /// @brief Compare two log_entry objects in descending order.
     ///
     /// The comparison is performed first using the write_version of each log_entry,
     /// which provides its own comparison functionality. If the write_versions are equal,
     /// then the key portion (excluding the storage_id prefix) is compared in descending order.
     bool operator()(const log_entry& a, const log_entry& b) const {
         write_version_type wa, wb;
         // Obtain the write_version information using the getter method.
         a.write_version(wa);
         b.write_version(wb);
 
         // Use the write_version_type's built-in comparison for descending order.
         // If the write_versions differ, the one with the larger value should come first.
         if (!(wa == wb)) {
             return wb < wa; // Returns true if 'a' has a larger write_version than 'b'.
         }
 
         // If write_versions are equal, compare the keys (excluding the storage_id prefix).
         std::string_view key_a(a.key_sid());
         std::string_view key_b(b.key_sid());
         key_a.remove_prefix(sizeof(storage_id_type)); // Remove the storage_id portion.
         key_b.remove_prefix(sizeof(storage_id_type));
 
         // Compare the keys in descending order.
         return key_b < key_a;
     }
 };
 
 } // namespace limestone::internal



