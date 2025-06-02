/*
 * Copyright 2023-2025 Project Tsurugi.
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

#include "manifest.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/file.h>
#include <unistd.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fstream>
#include <sstream>

#include "compaction_catalog.h"
#include "limestone_exception_helper.h"
#include "logging_helper.h"

namespace limestone::internal {

manifest::manifest()
    : format_version_(default_format_version)
    , persistent_format_version_(default_persistent_format_version)
    , instance_uuid_(generate_instance_uuid())
{}

manifest::manifest(std::string format_version, int persistent_format_version, std::string instance_uuid)
    : format_version_(std::move(format_version))
    , persistent_format_version_(persistent_format_version)
    , instance_uuid_(std::move(instance_uuid))
{}

void manifest::create_initial(const boost::filesystem::path& logdir) {
    real_file_operations default_ops;
    create_initial(logdir, default_ops);
}    

void manifest::create_initial(const boost::filesystem::path& logdir, file_operations& ops) {
    // Create a manifest instance with the default version information
    manifest m;

    boost::filesystem::path config = logdir / std::string(file_name);
    write_file_safely(config, m, ops);
}

void manifest::write_file_safely(
    const boost::filesystem::path& file_path,
    const limestone::internal::manifest& m,
    file_operations& ops
) {
    std::string manifest_str = m.to_json_string();

    FILE* strm = ops.fopen(file_path.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        std::string err_msg = "Failed to open file for writing: " + file_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    auto ret = ops.fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        std::string err_msg = "Failed to write to file: " + file_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fflush(strm) != 0) {
        std::string err_msg = "Failed to flush file buffer: " + file_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fsync(fileno(strm)) != 0) {
        std::string err_msg = "Failed to sync file to disk: " + file_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        std::string err_msg = "Failed to close file: " + file_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
}


int manifest::acquire_lock(const boost::filesystem::path& logdir) {
    real_file_operations default_ops;
    return acquire_lock(logdir, default_ops);
}

int manifest::acquire_lock(const boost::filesystem::path& logdir, file_operations& ops) {
    boost::filesystem::path manifest_path = logdir / std::string(file_name);
    int fd = ops.open(manifest_path.c_str(), O_RDWR);// NOLINT(hicpp-vararg, cppcoreguidelines-pro-type-vararg)
    if (fd == -1) {
        return -1;
    }

    if (ops.flock(fd, LOCK_EX | LOCK_NB) == -1) {
        ops.close(fd);
        return -1;
    }
    VLOG_LP(log_info) << "acquired lock on manifest file: " << manifest_path.string();
    return fd;
}

int manifest::is_supported_version(const boost::filesystem::path& manifest_path, std::string& errmsg) {
    std::ifstream istrm(manifest_path.string());
    if (!istrm) {
        errmsg = "cannot open for read " + manifest_path.string();
        return 0;
    }

    try {
        std::string json_str((std::istreambuf_iterator<char>(istrm)), std::istreambuf_iterator<char>());
        manifest m = manifest::from_json_string(json_str);
        int v = m.get_persistent_format_version();
        if (1 <= v && v <= manifest::default_persistent_format_version) {
            return v;  // Supported version
        }
        errmsg = "version mismatch: version " + std::to_string(v) + ", server supports versions 1 through 4";
        return 0;
    } catch (const std::exception& e) {
        errmsg = "invalid manifest file, parse error: ";
        errmsg.append(e.what());
        return -1;
    }
}

void manifest::check_and_migrate(const boost::filesystem::path& logdir) {
    real_file_operations default_ops;
    check_and_migrate(logdir, default_ops);
}

void manifest::check_and_migrate(const boost::filesystem::path& logdir, file_operations& ops) {
    boost::filesystem::path manifest_path = logdir / std::string(file_name);
    boost::filesystem::path manifest_backup_path = logdir / std::string(backup_file_name);
    boost::system::error_code ec;

    boost::optional<manifest> manifest_backup = manifest::load_manifest_from_path(manifest_backup_path, ops);
    boost::optional<manifest> manifest = manifest::load_manifest_from_path(manifest_path, ops);

    if (!manifest && manifest_backup) {
        VLOG_LP(log_info) << "Manifest file is missing or corrupted, but a backup file exists at " << manifest_backup_path.string()
                          << ". Recovering manifest from backup by renaming it to " << manifest_path.string();
        ops.rename(manifest_backup_path, manifest_path, ec);
        if (ec) {
            std::string err_msg = "Failed to rename manifest backup from " + manifest_backup_path.string() + " to " + manifest_path.string();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
        manifest = manifest_backup;
    }

    if (!exists_path(manifest_path)) {
        VLOG_LP(log_info) << "No manifest file in logdir, maybe v0";
        THROW_LIMESTONE_EXCEPTION(std::string(version_error_prefix) + " (version mismatch: version 0, server supports version 1)");
    }

    if (!manifest) {
        std::string err_msg = "Manifest file exists but is corrupted or cannot be parsed: " + manifest_path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }

    if (manifest_backup) {
        VLOG_LP(log_info) << "Removing backup manifest file: " << manifest_backup_path.string();
        ops.remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
    }


    
    std::string errmsg;
    int vc = is_supported_version(manifest_path, errmsg);
    if (vc == 0) {
        LOG(ERROR) << version_error_prefix << " (" << errmsg << ")";
        THROW_LIMESTONE_EXCEPTION("logdir version mismatch");
    }


    int persistent_version = manifest->get_persistent_format_version();
    if (persistent_version < default_persistent_format_version) {
        VLOG_LP(log_info) << "Migrating manifest file (safe double-write: backup then main)"
                          << " from version " << persistent_version << " to " << default_persistent_format_version;
        migrate_manifest(manifest_path, manifest_backup_path, *manifest, ops);
    }
}

// NOTE:
// If this function (migrate_manifest) is interrupted before completion (e.g., process kill),
// one of the following states may result:
//
// 1. BothOld          : both manifest and backup files have old content
// 2. MainOldBackupNew : manifest file has old content, backup file has new content
// 3. BothNew          : both manifest and backup files have new content
// 4. MainNew          : manifest file has new content, backup file deleted (normal case)
// 5. BackupCorrupt    : backup file is corrupt, manifest file has old content
// 6. MainCorrupt      : manifest file is corrupt, backup file has new content
void manifest::migrate_manifest(
    const boost::filesystem::path& manifest_path,
    const boost::filesystem::path& manifest_backup_path,
    const manifest& old_manifest,
    file_operations& ops
) {
    boost::system::error_code ec;
    std::string instance_uuid = old_manifest.get_instance_uuid();
    if (instance_uuid.empty()) {
        instance_uuid = generate_instance_uuid();
    }

    manifest new_manifest(
        default_format_version,
        default_persistent_format_version,
        instance_uuid
    );

    write_file_safely(manifest_backup_path, new_manifest, ops);
    write_file_safely(manifest_path, new_manifest, ops);

    ops.remove(manifest_backup_path, ec);
    if (ec) {
        std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string() + ". Error: " + ec.message();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
}


bool manifest::exists_path(const boost::filesystem::path& path) {
    real_file_operations default_ops;
    return exists_path_with_ops(path, default_ops);
}


bool manifest::exists_path_with_ops(const boost::filesystem::path& path, file_operations& ops) {
    boost::system::error_code ec;
    bool ret = ops.exists(path, ec);
    if (!ret && ec != boost::system::errc::no_such_file_or_directory) {
        std::string err_msg = "Failed to check if file exists: " + path.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
    return ret;
}

// setter/getter
const std::string& manifest::get_format_version() const {
    return format_version_;
}
int manifest::get_persistent_format_version() const {
    return persistent_format_version_;       
}
const std::string& manifest::get_instance_uuid() const {
    return instance_uuid_;
}

std::string manifest::to_json_string() const {
    nlohmann::json j = {
        {"format_version", format_version_},
        {"persistent_format_version", persistent_format_version_}
    };
    if (format_version_ != "1.0") {
        j["instance_uuid"] = instance_uuid_;
    }
    return j.dump();
}



manifest manifest::from_json_string(const std::string& json_str) {
    try {
        nlohmann::json j = nlohmann::json::parse(json_str);

        std::string format_version;
        int persistent_format_version = 0;
        std::string instance_uuid;

        try {
            format_version = j.at("format_version").get<std::string>();
        } catch (const std::exception& e) {
            LOG_AND_THROW_EXCEPTION(std::string("missing or invalid 'format_version' in manifest json: ") + e.what());
        }
        try {
            persistent_format_version = j.at("persistent_format_version").get<int>();
        } catch (const std::exception& e) {
            LOG_AND_THROW_EXCEPTION(std::string("missing or invalid 'persistent_format_version' in manifest json: ") + e.what());
        }
        if (format_version == "1.0") {
            instance_uuid = "";
        } else {
            try {
                instance_uuid = j.at("instance_uuid").get<std::string>();
            } catch (const std::exception& e) {
                LOG_AND_THROW_EXCEPTION(std::string("missing or invalid 'instance_uuid' in manifest json: ") + e.what());
            }
        }
        return {format_version, persistent_format_version, instance_uuid};
    } catch (const nlohmann::json::parse_error& e) {
        LOG_AND_THROW_EXCEPTION(std::string("failed to parse manifest json (invalid JSON format): ") + e.what());
    }
}

boost::optional<manifest> manifest::load_manifest_from_path(const boost::filesystem::path& path, file_operations& ops) {
    if (!exists_path_with_ops(path, ops)) {
        return boost::none;
    }
    try {
        std::ifstream istrm(path.string());
        if (!istrm) {
            return boost::none;
        }
        std::string json_str((std::istreambuf_iterator<char>(istrm)), std::istreambuf_iterator<char>());
        manifest m = manifest::from_json_string(json_str);
        return m;
    } catch (...) {
        return boost::none;
    }
}

std::string manifest::generate_instance_uuid() {
    return boost::uuids::to_string(boost::uuids::random_generator()());
}


} // namespace limestone::internal
