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

namespace limestone::internal {
using namespace limestone::api;

// moved from datastore.h
/**
 * @brief name of a file to record durable epoch
 */
static constexpr const std::string_view epoch_file_name = "epoch";

// moved from log_channel.h
/**
 * @brief prefix of pwal file name
 */
static constexpr const std::string_view log_channel_prefix = "pwal_";

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

void check_and_migrate_logdir_format(const boost::filesystem::path& logdir);

// from datastore_restore.cpp

status purge_dir(const boost::filesystem::path& dir);

// from datastore_snapshot.cpp

void create_compact_pwal(
    const boost::filesystem::path& from_dir, 
    const boost::filesystem::path& to_dir, 
    int num_worker,
    const std::set<std::string>& file_names = std::set<std::string>());

// filepath.cpp

void remove_trailing_dir_separators(boost::filesystem::path& p);
boost::filesystem::path make_tmp_dir_next_to(const boost::filesystem::path& target_dir, const char* suffix);

}
