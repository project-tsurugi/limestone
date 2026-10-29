/*
 * Copyright 2022-2026 Project Tsurugi.
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

 #include <optional>
 #include <set>
 #include <string>
 #include <functional> // std::reference_wrapper
 #include <boost/filesystem.hpp>
 #include "blob_file_gc_snapshot.h"
 #include "sort/sort_settings.h"
 #include "limestone/api/epoch_id_type.h"
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
       gc_snapshot_(nullptr)
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
       gc_snapshot_(nullptr)
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
       gc_snapshot_(nullptr)
     {}
 
     // Constructor: to_dir provided, file set available, GC enabled.
     // Note: The blob_file_gc_snapshot is provided externally by reference.
     compaction_options(
         boost::filesystem::path from,
         boost::filesystem::path to,
         int workers,
         std::set<std::string> file_names,
         std::unique_ptr<blob_file_gc_snapshot> gc_snapshot
     )
     : from_dir_(std::move(from)),
       to_dir_(std::move(to)),
       num_worker_(workers),
       file_names_(std::move(file_names)),
       has_file_set_(true),
       gc_snapshot_(std::move(gc_snapshot))
     {}
 
     // Getter for from_dir.
     [[nodiscard]] const boost::filesystem::path& get_from_dir() const { return from_dir_; }
 
     // Getter for to_dir.
     [[nodiscard]] const boost::filesystem::path& get_to_dir() const { return to_dir_; }
 
     // Getter for num_worker (the number of threads of the sort: the scan and the sort per thread).
     [[nodiscard]] int get_num_worker() const { return num_worker_; }
 
     // Getter for file_names.
     [[nodiscard]] const std::set<std::string>& get_file_names() const { return file_names_; }
 
     // Returns true if a file set is configured.
     [[nodiscard]] bool has_file_set() const { return has_file_set_; }

     // Sets the compaction boundary epoch. Online compaction passes the rotation
     // boundary here. When it is not set, the scan uses the last durable epoch in
     // the directory as the boundary (the offline compaction path).
     void set_boundary_epoch(limestone::api::epoch_id_type boundary_epoch) { boundary_epoch_ = boundary_epoch; }

     // Getter for boundary_epoch.
     [[nodiscard]] std::optional<limestone::api::epoch_id_type> get_boundary_epoch() const { return boundary_epoch_; }

     // Sets the file names of the compaction output (compacted and carry).
     // When unset, the compacted file is written with the fixed name of the
     // migration rule and no carry file is written.
     void set_output_file_names(std::string compacted_file_name, std::string carry_file_name) {
         compacted_file_name_ = std::move(compacted_file_name);
         carry_file_name_ = std::move(carry_file_name);
     }

     // Getter for compacted_file_name.
     [[nodiscard]] const std::optional<std::string>& get_compacted_file_name() const { return compacted_file_name_; }

     // Getter for carry_file_name.
     [[nodiscard]] const std::optional<std::string>& get_carry_file_name() const { return carry_file_name_; }

     // Takes the compacted WAL file of the current generation (recorded by the catalog; nullopt = none) out of
     // the scanned files (file_names) and makes it an input of the final merge. Does nothing if file_name is
     // nullopt or is not in file_names (it is not an input of this compaction).
     void set_compacted_input_file_name(const std::optional<std::string>& file_name) {
         if (file_name.has_value() && file_names_.erase(file_name.value()) > 0) {
             compacted_input_file_name_ = file_name;
         }
     }

     // Getter for compacted_input_file_name (nullopt = the final merge has no compacted WAL file).
     [[nodiscard]] const std::optional<std::string>& get_compacted_input_file_name() const {
         return compacted_input_file_name_;
     }

     // Settings of the sort other than the threads (the threads are num_worker). The defaults of sort_settings
     // and from_dir are used if not set. The values must have been checked at the entry point (the construction
     // of the datastore, dblogutil).
     void set_sort_settings(std::size_t arena_size_bytes, std::size_t merge_fanin,
                            boost::filesystem::path work_dir_parent) {
         sort_arena_size_bytes_ = arena_size_bytes;
         sort_merge_fanin_ = merge_fanin;
         sort_work_dir_parent_ = std::move(work_dir_parent);
     }

     // Getter for the settings of the sort (the threads are num_worker).
     [[nodiscard]] sort_settings get_sort_settings() const {
         return {num_worker_, sort_arena_size_bytes_, sort_merge_fanin_};
     }

     // Getter for the parent directory of the working directory of the sort (from_dir if not set).
     [[nodiscard]] const boost::filesystem::path& get_sort_work_dir_parent() const {
         return sort_work_dir_parent_.empty() ? from_dir_ : sort_work_dir_parent_;
     }

     // Check if GC is enabled.
     [[nodiscard]] bool is_gc_enabled() const { return static_cast<bool>(gc_snapshot_); }

     // Getter for gc_snapshot.
     // It is caller's responsibility to ensure GC is enabled before calling.
     [[nodiscard]] blob_file_gc_snapshot& get_gc_snapshot() const {
         if (!gc_snapshot_) {
             throw std::logic_error("GC is not enabled");
         }
         return *gc_snapshot_;
     }

 private:
     // Basic compaction settings.
     boost::filesystem::path from_dir_;
     boost::filesystem::path to_dir_;
     int num_worker_;

     // File set for compaction.
     std::set<std::string> file_names_;
     bool has_file_set_;

     // Garbage collection settings.
     std::unique_ptr<blob_file_gc_snapshot> gc_snapshot_;

     // Compaction boundary epoch (nullopt = the durable epoch serves as the boundary).
     std::optional<limestone::api::epoch_id_type> boundary_epoch_{};

     // File names of the compaction output (nullopt = fixed name for compacted, no carry)
     std::optional<std::string> compacted_file_name_{};
     std::optional<std::string> carry_file_name_{};

     // The compacted WAL file of the current generation given to the final merge (nullopt = none)
     std::optional<std::string> compacted_input_file_name_{};

     // Settings of the sort other than the threads (defaults of sort_settings; empty work dir = from_dir)
     std::size_t sort_arena_size_bytes_{sort_settings{}.arena_size_bytes};
     std::size_t sort_merge_fanin_{sort_settings{}.merge_fanin};
     boost::filesystem::path sort_work_dir_parent_{};
 };

 }  // namespace limestone::internal
