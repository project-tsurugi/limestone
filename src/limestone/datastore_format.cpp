/*
 * Copyright 2023-2023 Project Tsurugi.
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
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <nlohmann/json.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>

#include "limestone_exception_helper.h"
#include "compaction_catalog.h"
#include "internal.h"
#include "log_entry.h"



namespace limestone::internal {
using namespace limestone::api;

// Create or initialize the manifest file in the specified log directory
// This function is used during logdir setup or when migrating logdir formats.
void setup_initial_logdir(const boost::filesystem::path& logdir) {
    // Create manifest file
    nlohmann::json manifest_v2 = {
        { "format_version", "1.0" },
        { "persistent_format_version", 4}
    };
    boost::filesystem::path config = logdir / std::string(manifest_file_name);
    FILE* strm = fopen(config.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        std::string err_msg = "Failed to open file for writing: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    std::string manifest_str = manifest_v2.dump(4);
    auto ret = fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        std::string err_msg = "Failed to write to file: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (fflush(strm) != 0) {
        std::string err_msg = "Failed to flush file buffer: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (fsync(fileno(strm)) != 0) {
        std::string err_msg = "Failed to sync file to disk: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        std::string err_msg = "Failed to close file: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    // Create compaction catalog file if it does not exist
    boost::filesystem::path catalog_path = logdir / compaction_catalog::get_catalog_filename();
    if (!exists_path(catalog_path)) {
        compaction_catalog catalog(logdir);
        catalog.update_catalog_file(0, 0, {}, {});
    }
}

static constexpr const char *version_error_prefix = "/:limestone unsupported dbdir persistent format version: "
    "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/upgrade-guide.md";

int is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg) {
    std::ifstream istrm(manifest_path.string());
    if (!istrm) {
        errmsg = "cannot open for read " + manifest_path.string();
        return 0;
    }
    nlohmann::json manifest;
    try {
        istrm >> manifest;
        auto version = manifest["persistent_format_version"];
        if (version.is_number_integer()) {
            int v = version;
            if (1 <= v && v <= 4) {
                return v;  // supported
            }
            errmsg = "version mismatch: version " + version.dump() + ", server supports versions 1 through 4";
            return 0;
        }
        errmsg = "invalid manifest file, invalid persistent_format_version: " + version.dump();
        return -1;
    } catch (nlohmann::json::exception& e) {
        errmsg = "invalid manifest file, JSON parse error: ";
        errmsg.append(e.what());
        return -1;
    }
}

bool exists_path(const boost::filesystem::path& path) {
    boost::system::error_code ec;
    bool ret = boost::filesystem::exists(path, ec);
    if (!ret && ec != boost::system::errc::no_such_file_or_directory) {
        std::string err_msg = "Failed to check if file exists: " + path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
    return ret;
}


void check_and_migrate_logdir_format(const boost::filesystem::path& logdir) {
    boost::filesystem::path manifest_path = logdir / std::string(manifest_file_name);
    boost::filesystem::path manifest_backup_path = logdir / std::string(manifest_file_backup_name);
    boost::system::error_code ec;

    if (!exists_path(manifest_path) && exists_path(manifest_backup_path)) {
        VLOG_LP(log_info) << "Manifest file is missing, but a backup file exists at " << manifest_backup_path.string()
                          << ". Using the backup file as the manifest by renaming it to " << manifest_path.string();
        boost::filesystem::rename(manifest_backup_path, manifest_path, ec);
        if (ec) {
            std::string err_msg =
                "Failed to rename manifest backup from " + manifest_backup_path.string() + " to " + manifest_path.string();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
    }

    if (exists_path(manifest_path) && exists_path(manifest_backup_path)) {
        VLOG_LP(log_info) << "both manifest and backup manifest file exists, removing backup manifest file";
        boost::filesystem::remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
    }

    if (!exists_path(manifest_path)) {
        VLOG_LP(log_info) << "no manifest file in logdir, maybe v0";
        THROW_LIMESTONE_EXCEPTION(std::string(version_error_prefix) + " (version mismatch: version 0, server supports version 1)");
    }
    std::string errmsg;
    int vc = is_supported_version(manifest_path, errmsg);
    if (vc == 0) {
        LOG(ERROR) << version_error_prefix << " (" << errmsg << ")";
        THROW_LIMESTONE_EXCEPTION("logdir version mismatch");
    }
    if (vc < 0) {
        VLOG_LP(log_info) << errmsg;
        LOG(ERROR) << "/:limestone dbdir is corrupted, can not use.";
        THROW_LIMESTONE_EXCEPTION("logdir corrupted");
    }
    if (vc < 4) {
        // migrate to version 4
        VLOG_LP(log_info) << "migrating from version " << vc << " to version 4";
        boost::filesystem::rename(manifest_path, manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to rename manifest file: " + manifest_path.string() + " to " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
        setup_initial_logdir(logdir);
        VLOG_LP(log_info) << "migration done";
        boost::filesystem::remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
    }
}

int acquire_manifest_lock(const boost::filesystem::path& logdir) {
    boost::filesystem::path manifest_path = logdir / std::string(manifest_file_name);

    int fd = ::open(manifest_path.string().c_str(), O_RDWR); // NOLINT(hicpp-vararg, cppcoreguidelines-pro-type-vararg)
    if (fd == -1) {
        return -1;
    }

    if (::flock(fd, LOCK_EX | LOCK_NB) == -1) {
        ::close(fd);
        return -1;
    }
    VLOG_LP(log_info) << "acquired lock on manifest file: " << manifest_path.string();
    return fd;
}

} // namespace limestone::internal
