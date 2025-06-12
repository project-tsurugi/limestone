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
#include "manifest.h"



namespace limestone::internal {
using namespace limestone::api;



// Create or initialize the manifest file in the specified log directory
// This function is used during logdir setup or when migrating logdir formats.
void setup_initial_logdir(const boost::filesystem::path& logdir) {
    manifest::create_initial(logdir);
    ensure_compaction_catalog(logdir);

}


void check_and_migrate_logdir_format(const boost::filesystem::path& logdir) {
    manifest::check_and_migrate(logdir);
    ensure_compaction_catalog(logdir);
}

void ensure_compaction_catalog(const boost::filesystem::path& logdir) {
    boost::filesystem::path catalog_path = logdir / compaction_catalog::get_catalog_filename();
    if (!boost::filesystem::exists(catalog_path)) {
        compaction_catalog catalog(logdir);
        catalog.update_catalog_file(0, 0, {}, {});
    }
}

} // namespace limestone::internal
