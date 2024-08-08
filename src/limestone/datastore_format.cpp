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

#include "internal.h"
#include "log_entry.h"

namespace limestone::internal {
using namespace limestone::api;

// setup log-dir with no data
void setup_initial_logdir(const boost::filesystem::path& logdir) {
    nlohmann::json manifest_v2 = {
        { "format_version", "1.0" },
        { "persistent_format_version", 2}
    };
    boost::filesystem::path config = logdir / std::string(manifest_file_name);
    FILE* strm = fopen(config.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!strm) {
        LOG_LP(ERROR) << "fopen for write failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    std::string manifest_str = manifest_v2.dump(4);
    auto ret = fwrite(manifest_str.c_str(), manifest_str.length(), 1, strm);
    if (ret != 1) {
        LOG_LP(ERROR) << "fwrite failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fflush(strm) != 0) {
        LOG_LP(ERROR) << "fflush failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fsync(fileno(strm)) != 0) {
        LOG_LP(ERROR) << "fsync failed, errno = " << errno;
        throw std::runtime_error("I/O error");
    }
    if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "fclose failed, errno = " << errno;
        throw std::runtime_error("I/O error");
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
            if (version == 1 || version == 2) {
                return version;  // supported
            }
            errmsg = "version mismatch: version " + version.dump() + ", server supports version 1";
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
}

} // namespace limestone::internal
