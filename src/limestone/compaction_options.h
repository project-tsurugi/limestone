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

 #pragma once

 #include <set>
 #include <string>
 #include <functional> // std::reference_wrapper
 #include <boost/filesystem.hpp>
 #include "blob_file_gc_snapshot.h"
 #include "limestone/api/write_version_type.h"
 
 namespace limestone::internal {
 
 class compaction_options {
  public:
     // Constructor: Pre-compaction phase where to_dir is not provided.
     // File set is provided and GC is disabled.
     // In this constructor, "/not_exists_dir" is set for to_dir to indicate an error if used.
     compaction_options(
         boost::filesystem::path from,
         int workers,
         std::set<std::string> file_names
     )
     : from_dir_(std::move(from)),
       to_dir_("/not_exists_dir"),
       num_worker_(workers),
       file_names_(std::move(file_names)),
       has_file_set_(true),
       gc_snapshot_(std::nullopt)
     {}
 
     // Constructor: to_dir provided, GC disabled, no file set.
     compaction_options(
         boost::filesystem::path from,
         boost::filesystem::path to,
         int workers
     )
     : from_dir_(std::move(from)),
       to_dir_(std::move(to)),
       num_worker_(workers),
       has_file_set_(false),
       gc_snapshot_(std::nullopt)
     {}
 
     // Constructor: to_dir provided, file set available, GC disabled.
     compaction_options(
         boost::filesystem::path from,
         boost::filesystem::path to,
         int workers,
         std::set<std::string> file_names
     )
     : from_dir_(std::move(from)),
       to_dir_(std::move(to)),
       num_worker_(workers),
       file_names_(std::move(file_names)),
       has_file_set_(true),
       gc_snapshot_(std::nullopt)
     {}
 
     // Constructor: to_dir provided, file set available, GC enabled.
     // Note: The blob_file_gc_snapshot is provided externally by reference.
     compaction_options(
         boost::filesystem::path from,
         boost::filesystem::path to,
         int workers,
         std::set<std::string> file_names,
         blob_file_gc_snapshot& gc_snapshot
     )
     : from_dir_(std::move(from)),
       to_dir_(std::move(to)),
       num_worker_(workers),
       file_names_(std::move(file_names)),
       has_file_set_(true),
       gc_snapshot_(std::ref(gc_snapshot))
     {}
 
     // Getter for from_dir.
     [[nodiscard]] const boost::filesystem::path& get_from_dir() const { return from_dir_; }
 
     // Getter for to_dir.
     [[nodiscard]] const boost::filesystem::path& get_to_dir() const { return to_dir_; }
 
     // Getter for num_worker.
     [[nodiscard]] int get_num_worker() const { return num_worker_; }
 
     // Getter for file_names.
     [[nodiscard]] const std::set<std::string>& get_file_names() const { return file_names_; }
 
     // Returns true if a file set is configured.
     [[nodiscard]] bool has_file_set() const { return has_file_set_; }
 
     // Check if GC is enabled.
     [[nodiscard]] bool is_gc_enabled() const { return gc_snapshot_.has_value(); }
 
     // Getter for gc_snapshot.
     // It is caller's responsibility to ensure GC is enabled before calling.
     [[nodiscard]] blob_file_gc_snapshot& get_gc_snapshot() const { return gc_snapshot_.value().get(); }
 
  private:
     // Basic compaction settings.
     boost::filesystem::path from_dir_;
     boost::filesystem::path to_dir_;
     int num_worker_;
 
     // File set for compaction.
     std::set<std::string> file_names_;
     bool has_file_set_;
 
     // Garbage collection settings.
     std::optional<std::reference_wrapper<blob_file_gc_snapshot>> gc_snapshot_;
 };
 
 } // namespace limestone::internal
 