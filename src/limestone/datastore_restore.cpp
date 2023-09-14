/*
 * Copyright 2022-2023 tsurugi project.
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

namespace limestone::api {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    VLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << std::boolalpha << keep_backup;

    for (const std::filesystem::path& p : std::filesystem::directory_iterator(location_)) {
        if (!std::filesystem::is_directory(p)) {
            try {
                std::filesystem::remove(p);
            } catch (std::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    auto from_dir = std::filesystem::path(from);
    for (const std::filesystem::path& p : std::filesystem::directory_iterator(from_dir)) {
        try {
            std::filesystem::copy_file(p, location_ / p.filename());
        } catch (std::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << p.string();
            return status::err_permission_error;
        }
    }
    if (!keep_backup) {
        for (const std::filesystem::path& p : std::filesystem::directory_iterator(from_dir)) {
            try {
                std::filesystem::remove(p);
            } catch (std::filesystem::filesystem_error& ex) {
                LOG_LP(WARNING) << ex.what() << " file = " << p.string();
            }
        }
    }
    return status::ok;
}

// prusik era
status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries) {
    VLOG_LP(log_debug) << "restore (from prusik) begin, from directory = " << from;

    // purge logdir
    // FIXME: copied this code from (old) restore(), fix duplicate
    for (const std::filesystem::path& p : std::filesystem::directory_iterator(location_)) {
        if (!std::filesystem::is_directory(p)) {
            try {
                std::filesystem::remove(p);
            } catch (std::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    auto from_dir = std::filesystem::path(from);
    for (auto & ent : entries) {
        std::filesystem::path src{ent.source_path()};
        std::filesystem::path dst{ent.destination_path()};
        if (src.is_absolute()) {
            // use it
        } else {
            src = from_dir / src;
        }
        // TODO: location check (for security)
        // TODO: assert dst.is_relative()
        if (!std::filesystem::exists(src) || !std::filesystem::is_regular_file(src)) {
            LOG_LP(ERROR) << "file not found : file = " << src.string();
            return status::err_not_found;
        }
        try {
            std::filesystem::copy_file(src, location_ / dst);
        } catch (std::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }

    }
    return status::ok;
}

} // namespace limestone::api
