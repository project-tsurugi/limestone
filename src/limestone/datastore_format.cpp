/*
 * Copyright 2023-2026 Project Tsurugi.
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
#include "file_operations.h"
#include "internal.h"
#include "locator/locator_format.h"
#include "locator/locator_settings.h"
#include "locator/locator_writer.h"
#include "log_entry/log_entry.h"
#include "manifest.h"



namespace limestone::internal {
using namespace limestone::api;



// Create or initialize the manifest file in the specified log directory
// This function is used during logdir setup or when migrating logdir formats.
void setup_initial_logdir(const boost::filesystem::path& logdir) {
    manifest::create_initial(logdir);
    ensure_compaction_catalog(logdir);

}


manifest::migration_info check_and_migrate_logdir_format(const boost::filesystem::path& logdir) {
    real_file_operations ops;
    const manifest old_manifest = manifest::check_and_repair(logdir, ops);
    ensure_compaction_catalog(logdir);
    const int old_version = old_manifest.get_persistent_format_version();
    if (old_version < manifest::default_persistent_format_version) {
        // A log directory of an older version has no locator file for the compacted WAL file recorded by the catalog.
        // Generate it before rewriting the manifest. If the process stops during the generation, the manifest keeps
        // the old version and the next startup generates it again (an existing locator file may be incomplete, so
        // it is discarded)
        const compaction_catalog catalog = compaction_catalog::from_catalog_file(logdir);
        bool generated = false;
        for (const auto& info : catalog.get_compacted_files()) {
            const boost::filesystem::path locatee = logdir / info.get_file_name();
            const boost::filesystem::path locator_file =
                logdir / locator_format::locator_file_name(info.get_file_name());
            boost::system::error_code ec;
            boost::filesystem::remove(locator_file, ec);
            if (ec) {
                LOG_AND_THROW_IO_EXCEPTION("failed to remove the locator file: " + locator_file.string(), ec);
            }
            const std::size_t records =
                locator_writer::build_from_file(locatee, locator_file, locator_settings{}.record_interval_bytes);
            LOG_LP(INFO) << "generated the locator file: " << locator_file.string() << " (records: " << records << ")";
            generated = true;
        }
        if (generated && ops.fsync_directory(logdir) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("failed to fsync directory: " + logdir.string(), errno);
        }
        VLOG_LP(log_info) << "Migrating manifest file (safe double-write: backup then main)"
                          << " from version " << old_version << " to " << manifest::default_persistent_format_version;
        manifest::migrate_manifest(logdir / std::string(manifest::file_name),
                                   logdir / std::string(manifest::backup_file_name), old_manifest, ops);
    }
    return {old_version, manifest::default_persistent_format_version};
}

void ensure_compaction_catalog(const boost::filesystem::path& logdir) {
    boost::filesystem::path catalog_path = logdir / compaction_catalog::get_catalog_filename();
    boost::filesystem::path backup_path = logdir / compaction_catalog::get_catalog_backup_filename();
    // Create an empty catalog only when neither the catalog nor its backup exists.
    // "No catalog, backup present" is the window of a crash in the middle of a catalog
    // update (rename the catalog to the backup, then create the new catalog); creating
    // an empty catalog here would let from_catalog_file read the empty catalog
    // successfully, so the recovery from the backup (restore_from_backup) would never
    // fire.
    if (!boost::filesystem::exists(catalog_path) && !boost::filesystem::exists(backup_path)) {
        compaction_catalog catalog(logdir);
        catalog.update_catalog_file(0, 0, 0, {}, std::nullopt, {});
    }
}

} // namespace limestone::internal
