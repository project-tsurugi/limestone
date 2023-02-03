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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include <limestone/status.h>
#include "logging_helper.h"

#include "datastore.h"

namespace limestone::api::impl {

status datastore::restore(std::string_view from, bool keep_backup) const noexcept {
    DVLOG_LP(log_debug) << "restore begin, from directory = " << from << " , keep_backup = " << (keep_backup ? "true" : "false");

    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(location_), boost::filesystem::directory_iterator())) {
        if(!boost::filesystem::is_directory(p)) {
            try {
                boost::filesystem::remove(p);
            } catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    auto from_dir = boost::filesystem::path(std::string(from));
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        try {
            boost::filesystem::copy_file(p, location_ / p.filename());
        }
        catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << p.string();
            return status::err_permission_error;
        }
    }
    if (!keep_backup) {
        BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
            try {
                boost::filesystem::remove(p);
            }
            catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(WARNING) << ex.what() << " file = " << p.string();
            }
        }
    }
    return status::ok;
}

// prusik era
status datastore::restore(std::string_view from, std::vector<file_set_entry>& entries) {
    DVLOG_LP(log_debug) << "restore (from prusik) begin, from directory = " << from;

    // purge logdir
    // FIXME: copied from (old) restore(), fix duplicate
    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(location_), boost::filesystem::directory_iterator())) {
        if(!boost::filesystem::is_directory(p)) {
            try {
                boost::filesystem::remove(p);
            } catch (boost::filesystem::filesystem_error& ex) {
                LOG_LP(ERROR) << ex.what() << " file = " << p.string();
                return status::err_permission_error;
            }
        }
    }

    auto from_dir = boost::filesystem::path(std::string(from));
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
            boost::filesystem::copy_file(src, location_ / dst);
        } catch (boost::filesystem::filesystem_error& ex) {
            LOG_LP(ERROR) << ex.what() << " file = " << src.string();
            return status::err_permission_error;
        }

    }
    return status::ok;
}

api::restore_progress datastore::restore_status() const noexcept {
    // FIXME
    std::abort();
    return api::restore_progress::FIXME;
}

} // namespace limestone::api::impl
