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

#include <fcntl.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <set>
#include <vector>

#include <glog/logging.h>
#include <limestone/api/datastore.h>
#include <limestone/logging.h>
#include <limestone/status.h>

#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "file_operations.h"
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
        if (ent.destination_path().string() != internal::manifest::file_name) {
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
 * @brief A copier that copies the files of a backup to the restore location and persists them.
 *
 * Synchronizes the data of each copied file, and synchronizes the directories that received the files and the parents
 * of the created directories in finish(). A failure writes an ERROR log and is returned as a status.
 */
class restore_copier {
public:
    /**
     * @brief Creates a copier.
     * @param ops the file_operations used to synchronize the files and the directories
     */
    explicit restore_copier(file_operations& ops) : ops_(&ops) {}

    /**
     * @brief Copies one file of the backup: a BLOB file to the position given by the resolver (creating the
     *        directories if missing), and any other file to dst.
     * @return status::ok, or status::err_permission_error on failure
     */
    status copy_backup_file(const blob_file_resolver& resolver, const boost::filesystem::path& src,
                            const boost::filesystem::path& dst) {
        if (!resolver.is_blob_file(src)) {
            return copy(src, dst);
        }
        boost::filesystem::path blob_dst = resolver.resolve_path(resolver.extract_blob_id(src));
        if (auto rc = create_directories(blob_dst.parent_path()); rc != status::ok) {
            return rc;
        }
        return copy(src, blob_dst);
    }

    /**
     * @brief Persists the names of the copied files and the created directories by synchronizing their directories.
     * @return status::ok, or status::err_permission_error on failure
     */
    status finish() {
        for (const boost::filesystem::path& dir : dirs_to_sync_) {
            if (ops_->fsync_directory(dir) != 0) {
                int error_code = errno;
                LOG_LP(ERROR) << "failed to synchronize the directory: " << dir.string() << ": "
                              << std::strerror(error_code);  // NOLINT(concurrency-mt-unsafe)
                return status::err_permission_error;
            }
        }
        return status::ok;
    }

private:
    /**
     * @brief Copies src to dst (overwriting it), and synchronizes the data of dst.
     * @return status::ok, or status::err_permission_error on failure
     */
    status copy(const boost::filesystem::path& src, const boost::filesystem::path& dst) {
        try {
            std::filesystem::copy_file(std::filesystem::path{src.string()}, std::filesystem::path{dst.string()},
                                       std::filesystem::copy_options::overwrite_existing);
        } catch (std::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }
        int fd = ops_->open(dst.c_str(), O_RDONLY);
        if (fd < 0) {
            int error_code = errno;
            LOG_LP(ERROR) << "failed to open the restored file to synchronize it: " << dst.string() << ": "
                          << std::strerror(error_code);  // NOLINT(concurrency-mt-unsafe)
            return status::err_permission_error;
        }
        if (ops_->fsync(fd) != 0) {
            int error_code = errno;
            ops_->close(fd);
            LOG_LP(ERROR) << "failed to synchronize the restored file: " << dst.string() << ": "
                          << std::strerror(error_code);  // NOLINT(concurrency-mt-unsafe)
            return status::err_permission_error;
        }
        if (ops_->close(fd) != 0) {
            int error_code = errno;
            LOG_LP(ERROR) << "failed to close the restored file: " << dst.string() << ": "
                          << std::strerror(error_code);  // NOLINT(concurrency-mt-unsafe)
            return status::err_permission_error;
        }
        dirs_to_sync_.insert(dst.parent_path());
        return status::ok;
    }

    /**
     * @brief Creates dir and its missing ancestors.
     * @return status::ok, or status::err_permission_error on failure
     */
    status create_directories(const boost::filesystem::path& dir) {
        try {
            for (boost::filesystem::path p = dir; !p.empty() && !boost::filesystem::exists(p); p = p.parent_path()) {
                dirs_to_sync_.insert(p.parent_path());
            }
            boost::filesystem::create_directories(dir);
        } catch (const boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " dir = " << dir.string();
            return status::err_permission_error;
        }
        return status::ok;
    }

    file_operations* ops_;
    std::set<boost::filesystem::path> dirs_to_sync_{};
};

/**
 * @brief Copies backup files to the restore location.
 *
 * @param from_dir The backup directory path.
 * @param entries The list of file entries.
 * @param location The target restore location.
 * @param ops the file_operations used to persist the copied files
 * @return status::ok if copy succeeds, otherwise an error status.
 */
status copy_backup_files(const boost::filesystem::path& from_dir, const std::vector<file_set_entry>& entries,
                         const boost::filesystem::path& location, file_operations& ops) {
    blob_file_resolver resolver{location};
    restore_copier copier{ops};

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
        if (auto rc = copier.copy_backup_file(resolver, src, location / dst); rc != status::ok) {
            return rc;
        }
    }
    return copier.finish();
}



}  // namespace

namespace limestone::api {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    VLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << std::boolalpha << keep_backup;
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

    if (auto rc = internal::purge_dir(location_); rc != status::ok) { return rc; }

    restore_copier copier{impl_->file_ops()};
    try {
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
            if (auto rc = copier.copy_backup_file(resolver, p, location_ / p.filename()); rc != status::ok) {
                return rc;
            }
        }
    } catch (const boost::filesystem::filesystem_error& ex) {
        LOG_LP(ERROR) << "Failed to iterate directory: " << ex.what();
        return status::err_permission_error;
    }
    if (auto rc = copier.finish(); rc != status::ok) {
        return rc;
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
    if (auto rc = copy_backup_files(from_dir, entries, location_, impl_->file_ops()); rc != status::ok) { return rc; }

    return status::ok;
}

} // namespace limestone::api
