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
#include <limestone/api/datastore.h>
#include <limestone/logging.h>
#include <limestone/status.h>

#include "blob_file_resolver.h"
#include "internal.h"
#include "logging_helper.h"
#include "manifest.h"

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

} // namespace limestone::internal


namespace {
   
using namespace limestone;
using namespace limestone::internal;

status check_manifest(const boost::filesystem::path& manifest_path) {
    std::string ver_err;
    int vc = internal::manifest::is_supported_version(manifest_path, ver_err);
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

/**
 * @brief Validates manifest files in the given entries.
 *
 * @param from_dir The backup directory path.
 * @param entries The list of file entries.
 * @return status::ok if validation succeeds, otherwise an error status.
 */
status validate_manifest_files(const boost::filesystem::path& from_dir, const std::vector<file_set_entry>& entries) {
    int manifest_count = 0;
    for (auto & ent : entries) {
        if (ent.destination_path().string() != std::string(internal::manifest::file_name)) {
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
        if (auto rc = check_manifest(src); rc != status::ok) {
            return rc;
        }
        manifest_count++;
    }
    if (manifest_count < 1) {  // XXX: change to != 1 ??
        VLOG_LP(log_info) << "no manifest file in backup";
        LOG(ERROR) << version_error_prefix << " (version mismatch: version 0, server supports version 1)";
        return status::err_broken_data;
    }
    return status::ok;
}

/**
 * @brief Copies backup files to the restore location.
 *
 * @param from_dir The backup directory path.
 * @param entries The list of file entries.
 * @param location The target restore location.
 * @return status::ok if copy succeeds, otherwise an error status.
 */
status copy_backup_files(const boost::filesystem::path& from_dir, const std::vector<file_set_entry>& entries, const boost::filesystem::path& location) {
    blob_file_resolver resolver{location};

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
            if (!resolver.is_blob_file(src)) {
                // overwrite if destination exists
                boost::filesystem::copy_file(src, location / dst, boost::filesystem::copy_options::overwrite_existing);
            } else {
                boost::filesystem::path full_dst = resolver.resolve_path(resolver.extract_blob_id(src));
                boost::filesystem::path dst_dir = full_dst.parent_path();
                if (!boost::filesystem::exists(dst_dir)) {
                    boost::filesystem::create_directories(dst_dir);
                }
                // overwrite blob file if destination exists
                boost::filesystem::copy_file(src, full_dst, boost::filesystem::copy_options::overwrite_existing);
            }
        } catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }
}
    return status::ok;
}



}  // namespace

namespace limestone::api {

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
status datastore::restore(std::string_view from, bool keep_backup, bool purge_destination) const noexcept {
    VLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << std::boolalpha << keep_backup
                       << " , purge_destination = " << std::boolalpha << purge_destination;
    limestone::internal::blob_file_resolver resolver{location_};
    auto from_dir = boost::filesystem::path(std::string(from));

    // log_dir version check
    boost::filesystem::path manifest_path = from_dir / std::string(internal::manifest::file_name);
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
    if (auto rc = check_manifest(manifest_path); rc != status::ok) { return rc; }

    if (purge_destination) {
        if (auto rc = internal::purge_dir(location_); rc != status::ok) { return rc; }
    }

    try {
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
            try {
                // If this is the manifest file and we are NOT purging the destination,
                // skip copying it when destination already contains a manifest.
                if (p.filename() == boost::filesystem::path(std::string(internal::manifest::file_name))) {
                    boost::filesystem::path dst_manifest = location_ / std::string(internal::manifest::file_name);
                    try {
                        if (boost::filesystem::exists(dst_manifest)) {
                            VLOG_LP(log_info) << "skip copying manifest since destination already has one: " << dst_manifest.string();
                            continue;
                        }
                    } catch (const boost::filesystem::filesystem_error& ex) {
                        LOG_LP(ERROR) << "Filesystem error while checking destination manifest: " << ex.what()
                                       << " file = " << dst_manifest.string();
                        return status::err_permission_error;
                    }
                }

                if (!resolver.is_blob_file(p)) {
                    // overwrite destination file if exists
                    boost::filesystem::copy_file(p, location_ / p.filename(), boost::filesystem::copy_options::overwrite_existing);
                } else {
                    boost::filesystem::path full_dst = resolver.resolve_path(resolver.extract_blob_id(p));
                    boost::filesystem::path dst_dir = full_dst.parent_path();
                    if (!boost::filesystem::exists(dst_dir)) {
                        boost::filesystem::create_directories(dst_dir);
                    }
                    // overwrite blob file if exists
                    boost::filesystem::copy_file(p, full_dst, boost::filesystem::copy_options::overwrite_existing);
                }
            } catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Failed to iterate directory: " << ex.what();
        return status::err_permission_error;
    }
    try {
        if (!keep_backup) {
            for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
                try {
                    boost::filesystem::remove_all(p);
                } catch (boost::filesystem::filesystem_error& ex) {
                    LOG_LP(WARNING) << ex.what() << " file = " << p.string();
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

    if (auto rc = validate_manifest_files(from_dir, entries); rc != status::ok) { return rc; }
    if (auto rc = internal::purge_dir(location_); rc != status::ok) { return rc; }
    if (auto rc = copy_backup_files(from_dir, entries, location_); rc != status::ok) { return rc; }

    return status::ok;
}

} // namespace limestone::api
