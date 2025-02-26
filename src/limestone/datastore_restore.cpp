/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include <limestone/status.h>
#include "internal.h"

namespace limestone::internal {

static constexpr const char *version_error_prefix = "/:limestone unsupported backup persistent format version: "
    "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/upgrade-guide.md";

status purge_dir(const boost::filesystem::path& dir) {
    try {
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(dir)) {
            if (!boost::filesystem::is_directory(p)) {
                try {
                    boost::filesystem::remove(p);
                } catch (const boost::filesystem::filesystem_error& ex) {
                    LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                    return status::err_permission_error;
                }
            }
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Failed to iterate directory: " << ex.what() << " dir = " << dir.string();
        return status::err_permission_error;
    }
    return status::ok;
}

static status check_manifest(const boost::filesystem::path& manifest_path) {
    std::string ver_err;
    int vc = internal::is_supported_version(manifest_path, ver_err);
    if (vc == 0) {
        LOG(ERROR) << version_error_prefix << " (" << ver_err << ")";
        return status::err_broken_data;
    }
    if (vc < 0) {
        VLOG_LP(log_info) << ver_err;
        LOG(ERROR) << "/:limestone backup data is corrupted, can not use.";
        return status::err_broken_data;
    }
    return status::ok;
}

}  // namespace limestone::internal

namespace limestone::api {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    VLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << std::boolalpha << keep_backup;
    auto from_dir = boost::filesystem::path(std::string(from));

    // Log directory version check: ensure the manifest file exists.
    boost::filesystem::path manifest_path = from_dir / std::string(internal::manifest_file_name);
    try {
        if (!boost::filesystem::exists(manifest_path)) {
            VLOG_LP(log_info) << "no manifest file in backup";
            LOG(ERROR) << internal::version_error_prefix << " (version mismatch: version 0, server supports version 1)";
            return status::err_broken_data;
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Filesystem error: " << ex.what() << " file = " << manifest_path.string();
        return status::err_permission_error;
    }
    if (auto rc = internal::check_manifest(manifest_path); rc != status::ok) { return rc; }

    if (auto rc = internal::purge_dir(location_); rc != status::ok) { return rc; }

    // Recursively copy files from the backup directory (from_dir) to the current location.
    try {
        boost::system::error_code ec;
        for (const auto& entry : boost::filesystem::recursive_directory_iterator(from_dir)) {
            if (!boost::filesystem::is_regular_file(entry)) {
                continue;
            }
            // Compute relative path from the backup root (from_dir) to this file.
            boost::filesystem::path rel_path = boost::filesystem::relative(entry.path(), from_dir, ec);
            if (ec) {
                LOG_LP(ERROR) << "Failed to compute relative path for file: " 
                                << entry.path().string() << " : " << ec.message();
                return status::err_permission_error;
            }
            // Build the destination file path: location_ / relative path.
            boost::filesystem::path dest_file = location_ / rel_path;
            // Ensure that the destination directory exists.
            boost::filesystem::create_directories(dest_file.parent_path());
            try {
                boost::filesystem::copy_file(entry.path(), dest_file);
            } catch (const boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << entry.path().string();
                return status::err_permission_error;
            }
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Failed to iterate backup directory recursively: " << ex.what();
        return status::err_permission_error;
    }

    // If keep_backup is false, remove the backup files.
    try {
        if (!keep_backup) {
            for (const auto& entry : boost::filesystem::recursive_directory_iterator(from_dir)) {
                try {
                    boost::filesystem::remove_all(entry.path());
                } catch (const boost::filesystem::filesystem_error& ex) {
                    LOG_LP(WARNING) << ex.what() << " file = " << entry.path().string();
                }
            }
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Failed to iterate backup directory for removal: " << ex.what();
        return status::err_permission_error;
    }
    return status::ok;
}
    
// prusik erase
status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries) noexcept{
    VLOG_LP(log_debug) << "restore (from prusik) begin, from directory = " << from;
    auto from_dir = boost::filesystem::path(std::string(from));

    // log_dir version check
    int manifest_count = 0;
    for (auto & ent : entries) {
        if (ent.destination_path().string() != internal::manifest_file_name) {
            continue;
        }
        boost::filesystem::path src{ent.source_path()};
        if (src.is_absolute()) {
            // use it
        } else {
            src = from_dir / src;
        }
        try {
            if (!boost::filesystem::exists(src) || !boost::filesystem::is_regular_file(src)) {
                LOG_LP(ERROR) << "File not found or not a regular file: " << src.string();
                return status::err_not_found;
            }
        } catch (const boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << "Filesystem error: " << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }
        if (auto rc = internal::check_manifest(src); rc != status::ok) {
            return rc;
        }
        manifest_count++;
    }
    if (manifest_count < 1) {  // XXX: change to != 1 ??
        VLOG_LP(log_info) << "no manifest file in backup";
        LOG(ERROR) << internal::version_error_prefix << " (version mismatch: version 0, server supports version 1)";
        return status::err_broken_data;
    }

    if (auto rc = internal::purge_dir(location_); rc != status::ok) { return rc; }

    for (auto & ent : entries) {
        boost::filesystem::path src{ent.source_path()};
        boost::filesystem::path dst{ent.destination_path()};
        if (src.is_absolute()) {
            // use it
        } else {
            src = from_dir / src;
        }
        // TODO: location check (for security)
        // TODO: assert dst.is_relative()
        if (!boost::filesystem::exists(src) || !boost::filesystem::is_regular_file(src)) {
            LOG_LP(ERROR) << "file not found : file = " << src.string();
            return status::err_not_found;
        }
        try {
            boost::filesystem::path full_dst = location_ / dst;
            boost::filesystem::path dst_dir = full_dst.parent_path();
            if (!boost::filesystem::exists(dst_dir)) {
                boost::filesystem::create_directories(dst_dir);
            }            
            boost::filesystem::copy_file(src, location_ / dst);
        } catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }

    }
    return status::ok;
}

} // namespace limestone::api
