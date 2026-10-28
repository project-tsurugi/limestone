/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <optional>
#include <set>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "limestone_exception_helper.h"
#include "compaction_carry_writer.h"
#include "compaction_catalog.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "log_entry/log_entry_view.h"
#include "snapshot_impl.h"
#include "sort/sort_output_writer.h"
#include "sort/wal_sorter.h"

namespace  {
using namespace limestone;
using namespace limestone::internal;

/**
 * @brief Creates the dblog_scan that scans the input files.
 * @param options the target of the scan. If it has no set of files, the whole directory is scanned (an empty set
 *                scans nothing)
 * @return dblog_scan
 */
dblog_scan make_dblog_scan(compaction_options& options) {
    return options.has_file_set() ? dblog_scan{options.get_from_dir(), options} : dblog_scan{options.get_from_dir()};
}

/**
 * @brief Performs the scan of the sort.
 * @param logscan the dblog_scan with the target of the scan configured (the one passed to sorter)
 * @param sorter the wal_sorter that performs the scan
 * @param options the settings of the scan. If the boundary epoch is given, the scan of the compaction, otherwise the
 *                scan with the durable epoch as the boundary
 * @param on_normal_with_blob the argument of wal_sorter::scan()
 * @return the result of the scan
 * @throws limestone_exception if the inputs are malformed ("dblogdir is corrupted"), or reading the epoch file fails
 * @throws limestone_io_exception if an I/O operation fails (writing to sorting, for instance)
 */
wal_scan_result scan_wals(dblog_scan& logscan, wal_sorter& sorter, const compaction_options& options,
                          const wal_sorter::owned_entry_callback& on_normal_with_blob) {
    // Online compaction: the boundary is the rotation boundary, and the snippets beyond it are skipped (the files are
    // left unmodified, because those snippets are carried over into the carry file).
    // Otherwise: the boundary is the durable epoch, and the snippets beyond it (not made durable) are invalidated. The
    // inputs are the whole directory, so "every entry at or below the boundary is in the inputs" holds. The epoch is
    // read outside the try block so that an exception caused by the epoch file is not flattened into "dblogdir is
    // corrupted" by the catch below.
    const std::optional<epoch_id_type> compaction_boundary = options.get_boundary_epoch();
    epoch_id_type boundary_epoch{};
    beyond_boundary_handling handling{};
    if (compaction_boundary.has_value()) {
        boundary_epoch = compaction_boundary.value();
        handling = beyond_boundary_handling::skip;
    } else {
        boundary_epoch = logscan.last_durable_epoch_in_dir();
        handling = beyond_boundary_handling::invalidate;
    }
    try {
        return sorter.scan(boundary_epoch, handling, on_normal_with_blob);
    } catch (const limestone_io_exception&) {
        // an I/O failure is not a malformed input, so it is passed through (the online compaction retries it)
        throw;
    } catch (const limestone_exception& e) {
        VLOG_LP(log_info) << "failed to scan pwal files: " << e.what();
        LOG(ERROR) << "/:limestone recover process failed. "
                   << "(cause: corruption detected in transaction log data directory), "
                   << "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/troubleshooting-guide.md";
        LOG(ERROR) << "/:limestone dblogdir (transaction log directory): " << options.get_from_dir();
        THROW_LIMESTONE_EXCEPTION("dblogdir is corrupted");
    }
}

}  // namespace

namespace limestone::internal {

compaction_output_result create_compaction_output(compaction_options &options) {
    dblog_scan logscan = make_dblog_scan(options);
    sort_settings settings{};
    settings.threads = options.get_num_worker();
    wal_sorter sorter(logscan, settings, options.get_from_dir());
    // recording for the BLOB GC. gc_snapshot puts the entries into containers per thread, so it can be called
    // concurrently from the threads of the scan
    wal_sorter::owned_entry_callback on_normal_with_blob{};
    if (options.is_gc_enabled()) {
        on_normal_with_blob = [&options](const log_entry& entry) {
            options.get_gc_snapshot().sanitize_and_add_entry(entry);
        };
    }
    wal_scan_result scanned = scan_wals(logscan, sorter, options, on_normal_with_blob);

    boost::system::error_code error;
    const auto &to_dir = options.get_to_dir();
    const bool result_check = boost::filesystem::exists(to_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(to_dir, error);
        if (!result_mkdir || error) {
            LOG_AND_THROW_IO_EXCEPTION("fail to create directory " + to_dir.string(), error);
        }
    }

    std::string compacted_file_name =
        options.get_compacted_file_name().value_or(compaction_catalog::get_compacted_filename());
    boost::filesystem::path compacted_file = to_dir / boost::filesystem::path(compacted_file_name);
    VLOG_LP(log_info) << "generating compacted pwal file: " << compacted_file;
    // NOTE: the header epoch of the compacted file must not exceed the compaction
    // boundary (0 is the safest). max_appeared_epoch includes the epochs of the
    // snippets beyond the boundary, so using it as the header would make the next
    // scan skip the whole compacted file as a snippet beyond the boundary.
    // sort_output_writer writes the header with epoch 0.
    // On a failure of the writing, the destruction of the writer closes the file; the removal of the temporary file
    // and the retry on the next request are left to the caller. close() performs fsync so that the content reaches
    // the disk before the publishing rename.
    sort_output_writer output(compacted_file, "compacted file",
                              {log_entry::write_version_handling::oldest, false, true});
    // The compacted WAL file of the current generation is not scanned but is an input of the final merge. Its
    // normal_with_blob entries are also recorded for the BLOB GC (called in this thread)
    std::optional<sorted_file_merger::compacted_wal_input> compacted{};
    if (const auto& name = options.get_compacted_input_file_name(); name.has_value()) {
        sorted_file_merger::entry_callback on_compacted_blob{};
        if (options.is_gc_enabled()) {
            on_compacted_blob = [&options](const log_entry_view& entry) {
                options.get_gc_snapshot().sanitize_and_add_entry(log_entry{entry});
            };
        }
        compacted = sorted_file_merger::compacted_wal_input{options.get_from_dir() / name.value(), on_compacted_blob};
    }
    sorter.merge(compacted, [&output](const log_entry_view& entry) { output.write(entry); });
    output.close();
    if (options.is_gc_enabled()) {
        // detaches the container of the GC records of this thread (as the workers of the scan do per file)
        options.get_gc_snapshot().finalize_local_entries();
    }

    // Save the snippets beyond the boundary epoch into the carry file (only when both
    // the boundary and the carry output name are given, that is, on the online path).
    // Only the files whose per-file maximum epoch exceeds the boundary are re-read; the
    // current compacted file has header epoch 0, so it is naturally out of scope.
    bool carry_written = false;
    const std::optional<epoch_id_type> boundary_epoch_opt = options.get_boundary_epoch();
    const std::optional<std::string>& carry_file_name = options.get_carry_file_name();
    if (boundary_epoch_opt.has_value() && carry_file_name.has_value()) {
        epoch_id_type boundary_epoch = boundary_epoch_opt.value();
        std::vector<boost::filesystem::path> carry_inputs;
        for (const auto& [path, max_epoch] : logscan.get_max_epoch_per_file()) {
            if (max_epoch > boundary_epoch) {
                carry_inputs.push_back(path);
            }
        }
        if (!carry_inputs.empty()) {
            compaction_carry_writer writer(boundary_epoch, to_dir / carry_file_name.value());
            carry_written = writer.write(carry_inputs);
        }
    }

    return {scanned.max_blob_id, carry_written};
}

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location,
    file_operations& file_ops) {
    std::set<std::string> detached_pwals = compaction_catalog->get_detached_pwals();
    // The compacted files excluded from the snapshot input are those recorded in the
    // catalog, not a well-known name; a cursor reads them directly. A compaction output
    // absent from the catalog has been removed by the orphan removal at startup.
    std::set<std::string> compacted_filenames;
    for (auto const& info : compaction_catalog->get_compacted_files()) {
        compacted_filenames.insert(info.get_file_name());
    }
    std::set<std::string> filename_set;
    boost::system::error_code error;
    boost::filesystem::directory_iterator it(location, error);
    boost::filesystem::directory_iterator end;

    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to initialize directory iterator, path: " + location.string(), error);
    }

    for (; it != end; file_ops.directory_iterator_next(it, error)) {
        if (error) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to access directory entry, path: " + location.string(), error);
        }
        if (boost::filesystem::is_regular_file(it->path())) {
            std::string filename = it->path().filename().string();
            if (detached_pwals.find(filename) == detached_pwals.end()
                && filename != compaction_catalog::get_catalog_filename()
                && compacted_filenames.find(filename) == compacted_filenames.end()) {
                filename_set.insert(filename);
            }
        }
    }
    return filename_set;
}

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location) {
    real_file_operations file_ops;
    return assemble_snapshot_input_filenames(compaction_catalog, location, file_ops);
}

std::set<boost::filesystem::path> filter_epoch_files(const boost::filesystem::path& directory) {
    std::set<boost::filesystem::path> epoch_files;
    for (const auto& entry : boost::filesystem::directory_iterator(directory)) {
        if (entry.path().filename().string().rfind(epoch_file_name, 0) == 0) { 
            epoch_files.insert(entry.path());
        }
    }
    return epoch_files;
}

void cleanup_rotated_epoch_files(const boost::filesystem::path& directory) {
    // Retrieve all epoch files in the directory
    std::set<boost::filesystem::path> epoch_files = filter_epoch_files(directory);

    // Define the main epoch file path
    boost::filesystem::path main_epoch_file = directory / std::string(epoch_file_name);

    // Check if the main epoch file exists among the filtered files
    auto main_file_it = epoch_files.find(main_epoch_file);
    if (main_file_it == epoch_files.end()) {
        LOG_AND_THROW_EXCEPTION("Epoch file does not exist: " + main_epoch_file.string());
    }

    // Remove the main epoch file from the set of epoch files
    epoch_files.erase(main_file_it);

    // Iterate through the remaining epoch files and remove them
    for (const auto& file : epoch_files) {
        boost::system::error_code ec;
        boost::filesystem::remove(file, ec);
        if (ec) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to remove file: " + file.string() + ". Error: ", ec);
        }
    }
}

} // namespace limestone::internal

namespace limestone::api {
using namespace limestone::internal;
 
snapshot::~snapshot() = default;

blob_id_type datastore::create_snapshot_and_get_max_blob_id() {
    const auto& from_dir = location_;
    std::set<std::string> file_names = assemble_snapshot_input_filenames(compaction_catalog_, from_dir);
    compaction_options options(from_dir, recover_max_parallelism_, file_names);
    dblog_scan logscan = make_dblog_scan(options);
    sort_settings settings{};
    settings.threads = options.get_num_worker();
    wal_sorter sorter(logscan, settings, from_dir);
    wal_scan_result scanned = scan_wals(logscan, sorter, options, {});
    epoch_id_switched_.store(scanned.max_appeared_epoch);
    epoch_id_informed_.store(scanned.max_appeared_epoch);

    boost::filesystem::path sub_dir = location_ / boost::filesystem::path(std::string(snapshot::subdirectory_name_));
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(sub_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(sub_dir, error);
        if (!result_mkdir || error) {
            LOG_AND_THROW_IO_EXCEPTION("fail to create directory", error);
        }
    }

    boost::filesystem::path snapshot_file = sub_dir / boost::filesystem::path(std::string(snapshot::file_name_));
    VLOG_LP(log_info) << "generating snapshot file: " << snapshot_file;
    // remove_entries are needed for the cursor to hide the old values in the compacted WAL file. They are not written
    // if there is no compacted WAL file
    const bool should_write_remove_entry = !compaction_catalog_->get_compacted_files().empty();
    sort_output_writer output(snapshot_file, "snapshot file",
                              {log_entry::write_version_handling::keep, should_write_remove_entry, false});
    sorter.merge(std::nullopt, [&output](const log_entry_view& entry) { output.write(entry); });
    output.close();

    clear_storage = std::move(scanned.deletions);

    return scanned.max_blob_id;
}

} // namespace limestone::api
