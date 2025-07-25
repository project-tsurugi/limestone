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

#include "online_compaction.h"

#include <glog/logging.h>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>
#include "limestone_exception_helper.h"
#include "logging_helper.h"
#include "compaction_catalog.h"

namespace limestone::internal {

using namespace limestone::api;    

void safe_rename(const boost::filesystem::path& from, const boost::filesystem::path& to) {
    boost::system::error_code error;
    boost::filesystem::rename(from, to, error);
    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("fail to rename the file from: " + from.string() + ", to: " + to.string() , error);
    }
}

std::set<std::string> select_files_for_compaction(const std::set<boost::filesystem::path>& rotation_end_files, std::set<std::string>& detached_pwals) {
    std::set<std::string> need_compaction_filenames;
    for (const boost::filesystem::path& path : rotation_end_files) {
        std::string filename = path.filename().string();
        if (filename.substr(0, 4) == "pwal" && filename.length() > 9 && detached_pwals.find(filename) == detached_pwals.end()) {
            need_compaction_filenames.insert(filename);
            detached_pwals.insert(filename);
            VLOG_LP(log_debug) << "Selected file for compaction: " << filename;
        } else {
            std::string reason;
            if (filename.substr(0, 4) != "pwal") {
                reason = "does not start with 'pwal'";
            } else if (filename.length() <= 9) {
                reason = "filename length is 9 or less";
            } else {
                reason = "file is already detached";
            }
            VLOG_LP(log_debug) << "File skipped for compaction: " << filename << " (Reason: " << reason << ")";
        }
    }
    return need_compaction_filenames;
}

void ensure_directory_exists(const boost::filesystem::path& dir) {
    if (boost::filesystem::exists(dir)) {
        if (!boost::filesystem::is_directory(dir)) {
            LOG_AND_THROW_EXCEPTION("The path exists but is not a directory: " + dir.string());
        }
    } else {
        boost::system::error_code error;
        const bool result_mkdir = boost::filesystem::create_directory(dir, error);
        if (!result_mkdir || error) {
            LOG_AND_THROW_IO_EXCEPTION("failed to create directory: " + dir.string(), error);
        }
    }
}

void handle_existing_compacted_file(const boost::filesystem::path& location) {
    boost::filesystem::path compacted_file = location / compaction_catalog::get_compacted_filename();
    boost::filesystem::path compacted_prev_file = location / compaction_catalog::get_compacted_backup_filename();

    if (boost::filesystem::exists(compacted_file)) {
        if (boost::filesystem::exists(compacted_prev_file)) {
            LOG_AND_THROW_EXCEPTION("the file already exists: " + compacted_prev_file.string());
        }
        safe_rename(compacted_file, compacted_prev_file);
    }
}

std::set<std::string> get_files_in_directory(const boost::filesystem::path& directory) {
    std::set<std::string> files;
    boost::system::error_code error; 

    if (!boost::filesystem::exists(directory, error)) {
        LOG_AND_THROW_IO_EXCEPTION("Directory does not exist: " + directory.string(), error);
    }

    if (!boost::filesystem::is_directory(directory, error)) {
        LOG_AND_THROW_IO_EXCEPTION("The path exists but is not a directory: " + directory.string(), error.value());
    }

    for (boost::filesystem::directory_iterator it(directory, error), end; it != end && !error; it.increment(error)) {
        if (boost::filesystem::is_regular_file(it->path(), error)) {
            files.insert(it->path().filename().string());
        }
    }

    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("Error while iterating directory: " + directory.string(), error.value());
    }

    return files;
}


void remove_file_safely(const boost::filesystem::path& file) {
    boost::system::error_code error;
    boost::filesystem::remove(file, error);
    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to remove the file", error);
    }
}

}