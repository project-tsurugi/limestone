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

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <functional>
#include <optional>

#include "file_operations.h"
#include "limestone/api/blob_id_type.h"
#include "compaction_options.h"
#include "manifest.h"


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
 * @brief tells whether the file name matches the naming rule of a pre-rotation
 *        (unrotated) pwal (`pwal_NNNN`, 9 characters)
 * @note this check is dedicated to pwal files; epoch files etc. are out of scope
 * @param filename the file name to check (must not contain a directory part)
 * @return true if the name matches
 */
bool is_unrotated_pwal_name(std::string_view filename) noexcept;

/**
 * @brief renames the given pwal file to its rotated name
 *        (`<original name>.<unixtime_millis:14 digits>.<epoch>`), re-fetching
 *        the wall clock until the target name is free when it already exists
 * @note concurrent calls on the same file are not allowed; the caller must
 *       hold the exclusion (the single mutex of the rotation mechanism)
 * @param file the pwal file to rename
 * @param epoch the epoch part of the rotated name
 * @return the path of the renamed file
 * @throws limestone_io_exception if the rename or the existence check fails
 */
boost::filesystem::path rotate_pwal_file(boost::filesystem::path const& file, epoch_id_type epoch);

/**
 * @brief type of the testing hook; returning an error_code skips the rename and fails
 *        with that error_code, returning nullopt renames as usual
 */
using rotate_pwal_file_rename_hook =
    std::function<std::optional<boost::system::error_code>(const boost::filesystem::path& from, const boost::filesystem::path& to)>;

/**
 * @brief hook that replaces only the rename call of rotate_pwal_file
 * @note always empty in production
 */
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,fuchsia-statically-constructed-objects)
extern rotate_pwal_file_rename_hook rotate_pwal_file_rename_for_test;

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
 * @brief The result of producing the compaction output (the files written to the output directory).
 */
struct compaction_output_result {
    limestone::api::blob_id_type max_blob_id{};
    bool carry_written{};
};

/**
 * @brief Writes the compacted file into the output directory of options, then fsyncs
 *        and closes it. When both the boundary epoch and the carry output name are set,
 *        a carry file is also written if there is any snippet beyond the boundary.
 *
 * @param options The compaction options holding the input and output directories, the
 *                number of workers, the set of input files, the boundary epoch, the
 *                output file names and the GC settings.
 * @return The maximum blob ID and whether a carry file was written.
 */
compaction_output_result create_compaction_output(compaction_options &options);


std::set<boost::filesystem::path> filter_epoch_files(const boost::filesystem::path& directory);

std::set<std::string> assemble_snapshot_input_filenames(const std::unique_ptr<compaction_catalog>& compaction_catalog, const boost::filesystem::path& location,
                                                        file_operations& file_ops);

std::set<std::string> assemble_snapshot_input_filenames(const std::unique_ptr<compaction_catalog>& compaction_catalog, const boost::filesystem::path& location);

void cleanup_rotated_epoch_files(const boost::filesystem::path& directory);

// filepath.cpp

void remove_trailing_dir_separators(boost::filesystem::path& p);
boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix);

}  // namespace limestone::internal
