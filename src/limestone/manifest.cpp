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

 #include "manifest.h"
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <glog/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "compaction_catalog.h"

namespace limestone::internal {

void manifest::create_initial(const boost::filesystem::path& logdir) {
    real_file_operations default_ops;
    create_initial(logdir, default_ops);
}    

void manifest::create_initial(const boost::filesystem::path& logdir, file_operations& ops) {
    nlohmann::json manifest_json = {
        {"format_version", "1.0"},
        {"persistent_format_version", 4}
    };
    boost::filesystem::path config = logdir / std::string(file_name);

    FILE* strm = ops.fopen(config.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        std::string err_msg = "Failed to open file for writing: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    std::string manifest_str = manifest_json.dump(4);
    auto ret = ops.fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        std::string err_msg = "Failed to write to file: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fflush(strm) != 0) {
        std::string err_msg = "Failed to flush file buffer: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fsync(fileno(strm)) != 0) {
        std::string err_msg = "Failed to sync file to disk: " + config.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, errno);
    }
    if (ops.fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        std::string err_msg = "Failed to close file: " + config.string();
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

void manifest::check_and_migrate(const boost::filesystem::path& logdir) {
    real_file_operations default_ops;
    check_and_migrate(logdir, default_ops);
}

void manifest::check_and_migrate(const boost::filesystem::path& logdir, file_operations& ops) {
    boost::filesystem::path manifest_path = logdir / std::string(file_name);
    boost::filesystem::path manifest_backup_path = logdir / std::string(backup_file_name);
    boost::system::error_code ec;

    if (!exists_path(manifest_path) && exists_path(manifest_backup_path)) {
        VLOG_LP(log_info) << "Manifest file is missing, but a backup file exists at " << manifest_backup_path.string()
                          << ". Using the backup file as the manifest by renaming it to " << manifest_path.string();
        ops.rename(manifest_backup_path, manifest_path, ec);
        if (ec) {
            std::string err_msg =
                "Failed to rename manifest backup from " + manifest_backup_path.string() + " to " + manifest_path.string();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
    }

    if (exists_path(manifest_path) && exists_path(manifest_backup_path)) {
        VLOG_LP(log_info) << "both manifest and backup manifest file exists, removing backup manifest file";
        ops.remove(manifest_backup_path, ec);
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
        ops.rename(manifest_path, manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to rename manifest file: " + manifest_path.string() + " to " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
        create_initial(logdir);
        VLOG_LP(log_info) << "migration done";
        ops.remove(manifest_backup_path, ec);
        if (ec) {
            std::string err_msg = "Failed to remove backup manifest file: " + manifest_backup_path.string() + ". Error: " + ec.message();
            LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
        }
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
void manifest::set_format_version(const std::string& version) {
    format_version_ = version;
}
const std::string& manifest::get_format_version() const {
    return format_version_;
}
void manifest::set_persistent_format_version(int version) {
    persistent_format_version_ = version;   
}
int manifest::get_persistent_format_version() const {
    return persistent_format_version_;       
}

std::string manifest::to_json_string() const {
    nlohmann::json j = {
        {"format_version", format_version_},
        {"persistent_format_version", persistent_format_version_}
    };
    return j.dump();
}


manifest manifest::from_json_string(const std::string& json_str) {
    try {
        nlohmann::json j = nlohmann::json::parse(json_str);

        manifest m;
        try {
            m.set_format_version(j.at("format_version").get<std::string>());
        } catch (const std::exception& e) {
            LOG_AND_THROW_EXCEPTION(std::string("missing or invalid 'format_version' in manifest json: ") + e.what());
        }
        try {
            m.set_persistent_format_version(j.at("persistent_format_version").get<int>());
        } catch (const std::exception& e) {
            LOG_AND_THROW_EXCEPTION(std::string("missing or invalid 'persistent_format_version' in manifest json: ") + e.what());
        }
        return m;

    } catch (const nlohmann::json::parse_error& e) {
        LOG_AND_THROW_EXCEPTION(std::string("failed to parse manifest json (invalid JSON format): ") + e.what());
    }
}




} // namespace limestone::internal
