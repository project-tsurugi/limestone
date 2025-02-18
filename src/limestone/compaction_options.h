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

#include <optional>
#include <set>
#include <string>
#include <functional> // std::reference_wrapper
#include <boost/filesystem.hpp>
#include "blob_file_gc_snapshot.h"
#include "limestone/api/write_version_type.h"

namespace limestone::internal {

class compaction_options {
public:
    // Constructor: No file set, GC disabled
    compaction_options(
        const boost::filesystem::path& from,
        const boost::filesystem::path& to,
        int workers
    ) : from_dir_(from), to_dir_(to), num_worker_(workers),
        file_names_(), use_file_set_(false),
        gc_snapshot_(std::nullopt) // GC is disabled
    {}

    // Constructor: File set available, GC disabled
    compaction_options(
        const boost::filesystem::path& from,
        const boost::filesystem::path& to,
        int workers,
        std::set<std::string> file_names
    ) : from_dir_(from), to_dir_(to), num_worker_(workers),
        file_names_(file_names), use_file_set_(true),
        gc_snapshot_(std::nullopt) // GC is disabled
    {}

    // Constructor: File set available, GC enabled
    // Note: The blob_file_gc_snapshot is provided externally by reference.
    compaction_options(
        const boost::filesystem::path& from,
        const boost::filesystem::path& to,
        int workers,
        std::set<std::string> file_names,
        blob_file_gc_snapshot& gc_snapshot
    ) : from_dir_(from), to_dir_(to), num_worker_(workers),
        file_names_(file_names), use_file_set_(true),
        gc_snapshot_(std::ref(gc_snapshot))
    {}

    // Getters
    const boost::filesystem::path& get_from_dir() const { return from_dir_; }
    const boost::filesystem::path& get_to_dir() const { return to_dir_; }
    int get_num_worker() const { return num_worker_; }

    const std::set<std::string>& get_file_names() const { return file_names_; }
    bool is_using_file_set() const { return use_file_set_; }

    bool is_gc_enabled() const { return gc_snapshot_.has_value(); }
    const blob_file_gc_snapshot& get_gc_snapshot() const { return gc_snapshot_.value().get(); }

private:
    // Basic compaction settings
    boost::filesystem::path from_dir_;
    boost::filesystem::path to_dir_;
    int num_worker_;

    std::set<std::string> file_names_;
    bool use_file_set_;

    // Garbage collection settings
    std::optional<std::reference_wrapper<blob_file_gc_snapshot>> gc_snapshot_;
};

} // namespace limestone::internal
