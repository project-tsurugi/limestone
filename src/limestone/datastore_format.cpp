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

#include <boost/filesystem.hpp>
#include <nlohmann/json.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include <limestone/api/compaction_catalog.h>

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
        { "persistent_format_version", 2}
    };
    boost::filesystem::path config = logdir / std::string(manifest_file_name);
    FILE* strm = fopen(config.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        std::string err_msg = "Failed to open file for writing: " + config.string() + ". errno = " + std::to_string(errno);
        LOG_LP(ERROR) << err_msg;
        throw std::runtime_error(err_msg);
    }
    std::string manifest_str = manifest_v2.dump(4);
    auto ret = fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        std::string err_msg = "Failed to write to file: " + config.string() + ". errno = " + std::to_string(errno);
        LOG_LP(ERROR) << err_msg;
        throw std::runtime_error(err_msg);
    }
    if (fflush(strm) != 0) {
        std::string err_msg = "Failed to flush file buffer: " + config.string() + ". errno = " + std::to_string(errno);
        LOG_LP(ERROR) << err_msg;
        throw std::runtime_error(err_msg);
    }
    if (fsync(fileno(strm)) != 0) {
        std::string err_msg = "Failed to sync file to disk: " + config.string() + ". errno = " + std::to_string(errno);
        LOG_LP(ERROR) << err_msg;
        throw std::runtime_error(err_msg);
    }
    if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        std::string err_msg = "Failed to close file: " + config.string() + ". errno = " + std::to_string(errno);
        LOG_LP(ERROR) << err_msg;
        throw std::runtime_error(err_msg);
    }
    // Create compaction catalog file
    compaction_catalog catalog(logdir);
    catalog.update_catalog_file(0, {}, {});
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
            if (v == 1 || v == 2) {
                return v;  // supported
            }
            errmsg = "version mismatch: version " + version.dump() + ", server supports version 1 or 2";
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

void check_and_migrate_logdir_format(const boost::filesystem::path& logdir) {
    boost::filesystem::path manifest_path = logdir / std::string(manifest_file_name);
    boost::filesystem::path manifest_backup_path = logdir / std::string(manifest_file_backup_name);
    boost::system::error_code ec;

    if (!boost::filesystem::exists(manifest_path) && boost::filesystem::exists(manifest_backup_path)) {
        VLOG_LP(log_info) << "Manifest file is missing, but a backup file exists at " << manifest_backup_path.string()
                          << ". Using the backup file as the manifest by renaming it to " << manifest_path.string();
        boost::filesystem::rename(manifest_backup_path, manifest_path, ec);
        if (ec) {
            std::string err_msg =
                "Failed to rename manifest backup from " + manifest_backup_path.string() + " to " + manifest_path.string() + ". Error: " + ec.message();
            LOG(ERROR) << err_msg;
            throw std::runtime_error(err_msg);
        }
    }

    if (boost::filesystem::exists(manifest_path) && boost::filesystem::exists(manifest_backup_path)) {
        VLOG_LP(log_info) << "both manifest and backup manifest file exists, removing backup manifest file";
        boost::filesystem::remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG(ERROR) << err_msg;
            throw std::runtime_error(err_msg);
        }
    }

    if (!boost::filesystem::exists(manifest_path)) {
        VLOG_LP(log_info) << "no manifest file in logdir, maybe v0";
        LOG(ERROR) << version_error_prefix << " (version mismatch: version 0, server supports version 1)";
        throw std::runtime_error("logdir version mismatch");
    }
    std::string errmsg;
    int vc = is_supported_version(manifest_path, errmsg);
    if (vc == 0) {
        LOG(ERROR) << version_error_prefix << " (" << errmsg << ")";
        throw std::runtime_error("logdir version mismatch");
    }
    if (vc < 0) {
        VLOG_LP(log_info) << errmsg;
        LOG(ERROR) << "/:limestone dbdir is corrupted, can not use.";
        throw std::runtime_error("logdir corrupted");
    }
    if (vc == 1) {
        // migrate to version 2
        VLOG_LP(log_info) << "migrating from version 1 to version 2";
        boost::filesystem::rename(manifest_path, manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to rename manifest file: " + manifest_path.string() + " to " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG(ERROR) << err_msg;
            throw std::runtime_error(err_msg);
        }
        setup_initial_logdir(logdir);
        VLOG_LP(log_info) << "migration done";
        boost::filesystem::remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG(ERROR) << err_msg;
            throw std::runtime_error(err_msg);
        }
    }
}

} // namespace limestone::internal
