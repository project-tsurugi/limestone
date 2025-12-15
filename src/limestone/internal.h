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

#include <limestone/api/datastore.h>

#include <chrono>
#include <boost/filesystem.hpp>
#include <optional>

#include "file_operations.h"
#include "limestone/api/blob_id_type.h"
#include "compaction_options.h"
#include "manifest.h"


namespace limestone::internal {
using namespace limestone::api;

inline std::uint64_t now_nsec() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

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

void setup_initial_logdir(const boost::filesystem::path& logdir);

// Validates the manifest file in the specified log directory and performs repair or migration if necessary.
manifest::migration_info check_and_migrate_logdir_format(const boost::filesystem::path& logdir);

/**
 * @brief Ensures that the compaction catalog exists in the specified log directory.
 *
 * This function checks the presence of the compaction catalog in the given log directory.
 * If the catalog is missing or corrupted, it creates a new one to ensure proper log compaction
 * and recovery operations can be performed.
 *
 * @param logdir The path to the log directory where the compaction catalog should exist.
 */
void ensure_compaction_catalog(const boost::filesystem::path& logdir);


// from datastore_restore.cpp

status purge_dir(const boost::filesystem::path& dir);

// from datastore_snapshot.cpp

/**
 * @brief Creates a compacted PWAL (Persistent Write-Ahead Log) and retrieves the maximum blob ID.
 *
 * This function performs log compaction using the given compaction options. 
 * It processes the specified input directory, compacts the logs, and stores 
 * the result in the target directory.
 *
 * @param options The compaction options that specify source and destination directories, 
 *                number of workers, file set, and garbage collection settings.
 * @return The maximum blob ID found during the compaction process.
 */
limestone::api::blob_id_type create_compact_pwal_and_get_max_blob_id(compaction_options &options);


std::set<boost::filesystem::path> filter_epoch_files(const boost::filesystem::path& directory);

std::set<std::string> assemble_snapshot_input_filenames(const std::unique_ptr<compaction_catalog>& compaction_catalog, const boost::filesystem::path& location,
                                                        file_operations& file_ops);

std::set<std::string> assemble_snapshot_input_filenames(const std::unique_ptr<compaction_catalog>& compaction_catalog, const boost::filesystem::path& location);

void cleanup_rotated_epoch_files(const boost::filesystem::path& directory);

// filepath.cpp

void remove_trailing_dir_separators(boost::filesystem::path& p);
boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix);

}  // namespace limestone::internal
