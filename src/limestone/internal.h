/*
 * Copyright 2023-2024 Project Tsurugi.
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

#include <boost/filesystem.hpp>

#include <limestone/api/datastore.h>
#include "file_operations.h"

namespace limestone::internal {
using namespace limestone::api;

// moved from datastore.h
/**
 * @brief name of a file to record durable epoch
 */
static constexpr const std::string_view epoch_file_name = "epoch";

static constexpr const std::string_view tmp_epoch_file_name = ".epoch.tmp";


// moved from log_channel.h
/**
 * @brief prefix of pwal file name
 */
static constexpr const std::string_view log_channel_prefix = "pwal_";


/**
 * @brief The maximum number of entries allowed in an epoch file.
 * 
 * This constant defines the upper limit for the number of entries that can be stored
 * in a single epoch file. It is used to ensure that the file does not grow too large,
 * which could impact performance and manageability.
 */
static constexpr const int max_entries_in_epoch_file = 100;

// from dblog_scan.cpp

// return max epoch in file.
std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file);

// from datastore_format.cpp

inline constexpr const std::string_view manifest_file_name = "limestone-manifest.json";
inline constexpr const std::string_view manifest_file_backup_name = "limestone-manifest.json.back";

void setup_initial_logdir(const boost::filesystem::path& logdir);

/**
 * @returns positive-integer: ok supported, zero: not supported, negative-integer: error, corrupted
 */
int is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg);

// Validates the manifest file in the specified log directory and performs repair or migration if necessary.
void check_and_migrate_logdir_format(const boost::filesystem::path& logdir);

// Acquires an exclusive lock on the manifest file.
// Returns the file descriptor on success, or -1 on failure.
// Note: This function does not log errors or handle them internally.
//       The caller must check the return value and use errno for error handling.
int acquire_manifest_lock(const boost::filesystem::path& logdir);

// from datastore_restore.cpp

status purge_dir(const boost::filesystem::path& dir);

// from datastore_snapshot.cpp

void create_compact_pwal(
    const boost::filesystem::path& from_dir, 
    const boost::filesystem::path& to_dir, 
    int num_worker,
    const std::set<std::string>& file_names = std::set<std::string>());

std::set<boost::filesystem::path> filter_epoch_files(
    const boost::filesystem::path& directory);

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location,
    file_operations& file_ops);

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location);

void cleanup_rotated_epoch_files(
    const boost::filesystem::path& directory);

// filepath.cpp

void remove_trailing_dir_separators(boost::filesystem::path& p);
boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix);

}
