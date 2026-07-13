/*
 * Copyright 2024-2024 Project Tsurugi.
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

#include <sys/stat.h>

#include <iostream>
#include <stdlib.h>  // NOLINT(*-deprecated-headers): <cstdlib> does not provide std::mkdtemp
#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include "limestone/api/datastore.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "limestone_exception_helper.h"
#include "manifest.h"
#include "compaction_catalog.h"

// NOLINTBEGIN(performance-avoid-endl)

using namespace limestone::api;
using namespace limestone::internal;

// common options
DEFINE_string(epoch, "", "specify valid epoch upper limit");
DEFINE_int32(thread_num, 1, "specify thread num of scanning wal file");
DEFINE_bool(h, false, "display help message");
DEFINE_bool(verbose, false, "verbose");

// inspect, repair
DEFINE_bool(cut, false, "repair by cutting for error-truncate and error-broken");
DEFINE_string(rotate, "all", "rotate files");
DEFINE_string(output_format, "human-readable", "format of output (human-readable/machine-readable)");

// compaction
DEFINE_bool(force, false, "(subcommand compaction) skip start prompt");
DEFINE_bool(dry_run, false, "(subcommand compaction) dry run");
DEFINE_string(working_dir, "", "(subcommand compaction) working directory");
DEFINE_bool(make_backup, false, "(subcommand compaction) make backup of target dblogdir");

enum subcommand {
    cmd_inspect,
    cmd_repair,
    cmd_compaction,
};

namespace {

using namespace limestone;

void log_and_exit(int error) {
    VLOG(10) << "exiting with code " << error;
    exit(error);
}

void inspect(dblog_scan &ds, std::optional<epoch_id_type> epoch) {
    std::cout << "persistent-format-version: 1" << std::endl;
    epoch_id_type ld_epoch{};
    try {
        ld_epoch = ds.last_durable_epoch_in_dir();
    } catch (limestone_exception& ex) {
        LOG(ERROR) << "reading epoch file is failed: " << ex.what();
        log_and_exit(64);
    }
    std::cout << "durable-epoch: " << ld_epoch << std::endl;
    std::atomic_size_t count_normal_entry = 0;
    std::atomic_size_t count_remove_entry = 0;
    ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::report);
    ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::report);
    ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::report);
    ds.set_fail_fast(false);
    dblog_scan::parse_error::code max_ec{};
    epoch_id_type max_appeared_epoch = ds.scan_pwal_files(epoch.value_or(ld_epoch), [&](log_entry& e){
        if (e.type() == log_entry::entry_type::normal_entry) {
            VLOG(50) << "normal";
            count_normal_entry++;
        } else if (e.type() == log_entry::entry_type::remove_entry) {
            VLOG(50) << "remove";
            count_remove_entry++;
        } else {
            LOG(ERROR) << static_cast<int>(e.type());
        }
    }, [](log_entry::read_error& ec){
        VLOG(30) << "ERROR " << ec.value() << " : " << ec.message();
        return false;
    }, &max_ec);
    std::cout << "max-appeared-epoch: " << max_appeared_epoch << std::endl;
    std::cout << "count-durable-wal-entries: " << (count_normal_entry + count_remove_entry) << std::endl;
    VLOG(10) << "scan_pwal_files done, max_ec = " << max_ec;
    switch (max_ec) {
    case dblog_scan::parse_error::ok:
        std::cout << "status: OK" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::repaired:
    case dblog_scan::parse_error::broken_after_tobe_cut:
        LOG(FATAL) << "status: unreachable " << max_ec;
    case dblog_scan::parse_error::broken_after:
    case dblog_scan::parse_error::broken_after_marked:
    case dblog_scan::parse_error::nondurable_entries:
        std::cout << "status: auto-repairable" << std::endl;
        log_and_exit(1);  // FIXME: conflicts with gflags error code
    case dblog_scan::parse_error::corrupted_durable_entries:
    case dblog_scan::parse_error::unexpected:
        std::cout << "status: unrepairable" << std::endl;
        log_and_exit(2);
    case dblog_scan::parse_error::failed:
        std::cout << "status: cannot-check" << std::endl;
        log_and_exit(64);
    }
}

void repair(dblog_scan &ds, std::optional<epoch_id_type> epoch) {
    epoch_id_type ld_epoch{};
    if (epoch.has_value()) {
        ld_epoch = epoch.value();
    } else {
        try {
            ld_epoch = ds.last_durable_epoch_in_dir();
        } catch (limestone_exception& ex) {
            LOG(ERROR) << "reading epoch file is failed: " << ex.what();
            log_and_exit(64);
        }
        std::cout << "durable-epoch: " << ld_epoch << std::endl;
    }
    ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
    ds.set_process_at_truncated_epoch_snippet(FLAGS_cut ? dblog_scan::process_at_truncated::repair_by_cut : dblog_scan::process_at_truncated::repair_by_mark);
    ds.set_process_at_damaged_epoch_snippet(FLAGS_cut ? dblog_scan::process_at_damaged::repair_by_cut : dblog_scan::process_at_damaged::repair_by_mark);
    ds.set_fail_fast(false);

    VLOG(30) << "detach all pwal files";
    ds.detach_wal_files();
    ds.rescan_directory_paths();
    std::atomic_size_t count_wal_entry = 0;
    dblog_scan::parse_error::code max_ec{};
    ds.scan_pwal_files(ld_epoch, [&count_wal_entry](log_entry&){ count_wal_entry++; }, [](log_entry::read_error& e) -> bool {
        LOG_LP(ERROR) << "this pwal file is broken: " << e.message();
        return false;
    }, &max_ec);
    VLOG(10) << "scan_pwal_files done, max_ec = " << max_ec;
    VLOG(10) << "count-durable-wal-entries: " << count_wal_entry;
    switch (max_ec) {
    case dblog_scan::parse_error::ok:
        std::cout << "status: OK" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::repaired:
    case dblog_scan::parse_error::broken_after_marked:
        std::cout << "status: repaired" << std::endl;
        log_and_exit(0);
    case dblog_scan::parse_error::broken_after_tobe_cut:
        LOG(FATAL) << "status: unreachable " << max_ec;
    case dblog_scan::parse_error::broken_after:
    case dblog_scan::parse_error::nondurable_entries:
    case dblog_scan::parse_error::corrupted_durable_entries:
    case dblog_scan::parse_error::unexpected:
        std::cout << "status: unrepairable" << std::endl;
        log_and_exit(16);
    case dblog_scan::parse_error::failed:
        std::cout << "status: cannot-check" << std::endl;
        log_and_exit(64);
    }
}


boost::filesystem::path make_work_dir_next_to(const boost::filesystem::path& target_dir) {
    // assume: already checked existence and is_dir
    return make_tmp_dir_next_to(target_dir, ".work_XXXXXX");
}

boost::filesystem::path make_backup_dir_next_to(const boost::filesystem::path& target_dir) {
    return make_tmp_dir_next_to(target_dir, ".backup_XXXXXX");
}

/**
 * @brief tests whether two existing directories reside on the same filesystem.
 * @param a a path to an existing directory
 * @param b a path to an existing directory
 * @return true if both are on the same filesystem (same st_dev), false otherwise
 */
bool on_same_filesystem(boost::filesystem::path const& a, boost::filesystem::path const& b) {
    struct stat sa {};
    struct stat sb {};
    if (::stat(a.c_str(), &sa) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("stat failed: " + a.string(), errno);
    }
    if (::stat(b.c_str(), &sb) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("stat failed: " + b.string(), errno);
    }
    return sa.st_dev == sb.st_dev;
}

// Carry over the existing compaction catalog (and its backup) from from_dir into the
// work directory tmp, then register the freshly produced compacted file. tmp will
// replace from_dir, so carrying over the catalog preserves the high-water marks
// recorded by previous compactions: update_catalog_file keeps max_blob_id
// monotonically non-decreasing, so the freshly computed value (which reflects only
// blobs still referenced by live entries) never lowers it and blob IDs are never
// reused. If from_dir has no catalog (a directory that was never compacted), the empty
// catalog created by setup_initial_logdir(tmp) is used instead.
void carry_over_and_update_compaction_catalog(boost::filesystem::path const& from_dir, boost::filesystem::path const& tmp,
                                              epoch_id_type ld_epoch, blob_id_type max_blob_id) {
    auto copy_catalog_file = [&](const std::string& filename) {
        boost::filesystem::path src = from_dir / filename;
        boost::system::error_code ec;
        bool present = boost::filesystem::exists(src, ec);
        // A non-existent file is the normal case (boost reports it via ec as ENOENT).
        // Any other error (permission, I/O, ...) must not be silently ignored: skipping
        // the copy would drop the catalog carry-over and let later processing run on an
        // inconsistent state.
        if (ec && ec != boost::system::errc::no_such_file_or_directory) {
            LOG_AND_THROW_IO_EXCEPTION("failed to check existence of compaction catalog file: " + src.string(), ec);
        }
        if (present) {
            boost::filesystem::copy_file(src, tmp / filename, boost::filesystem::copy_options::overwrite_existing, ec);
            if (ec) {
                LOG_AND_THROW_IO_EXCEPTION("failed to copy compaction catalog file: " + src.string(), ec);
            }
        }
    };
    copy_catalog_file(compaction_catalog::get_catalog_filename());
    copy_catalog_file(compaction_catalog::get_catalog_backup_filename());

    VLOG_LP(log_info) << "updating compaction catalog in " << tmp;
    // tmp always has a catalog here (setup_initial_logdir created an empty one, possibly
    // overwritten by the carry-over above), so loading only fails when the carried-over
    // catalog and its backup are both unreadable. In that case abort with a clear message
    // rather than silently proceeding, which would lose the blob-id high-water mark.
    compaction_catalog catalog = [&]() {
        try {
            return compaction_catalog::from_catalog_file(tmp);
        } catch (const limestone_exception& ex) {
            LOG_AND_THROW_EXCEPTION(
                "the existing compaction catalog in " + from_dir.string() +
                " is unreadable and could not be recovered; offline compaction was aborted to"
                " avoid losing the blob-id high-water mark (cause: " + ex.what() + ")");
        }
    }();
    compacted_file_info compacted_file{compaction_catalog::get_compacted_filename(), 1};
    catalog.update_catalog_file(ld_epoch, max_blob_id, {compacted_file}, {});
}

/**
 * @brief copies the manifest file from the original log directory to the working directory.
 *
 * setup_initial_logdir() creates a brand-new manifest in the working directory, which would
 * replace the original instance_uuid with a freshly generated one. Overwrite it with the
 * original manifest so that the instance identity survives offline compaction. The manifest
 * in from_dir is guaranteed to exist and to be migrated to the current format version because
 * main() calls manifest::acquire_lock() and check_and_migrate_logdir_format() beforehand.
 * @param from_dir the original log directory
 * @param tmp the working directory the compacted log is assembled in
 */
void carry_over_manifest_file(
        boost::filesystem::path const& from_dir, boost::filesystem::path const& tmp) {
    boost::filesystem::path src = from_dir / std::string(manifest::file_name);
    boost::system::error_code ec;
    bool present = boost::filesystem::exists(src, ec);
    if (ec && ec != boost::system::errc::no_such_file_or_directory) {
        LOG_AND_THROW_IO_EXCEPTION(
                "failed to check existence of manifest file: " + src.string(), ec);
    }
    if (!present) {
        // must not happen: main() has already acquired the lock on this file
        LOG_AND_THROW_EXCEPTION("manifest file not found in dblogdir: " + src.string());
    }
    boost::filesystem::copy_file(src, tmp / std::string(manifest::file_name),
                                 boost::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("failed to copy manifest file: " + src.string(), ec);
    }
}

/**
 * @brief recursively copies a directory tree.
 * @param src the source directory (must exist)
 * @param dst the destination directory (created by this function)
 */
void copy_directory_recursively(
        boost::filesystem::path const& src, boost::filesystem::path const& dst) {
    boost::system::error_code ec;
    boost::filesystem::create_directories(dst, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("failed to create directory: " + dst.string(), ec);
    }
    boost::filesystem::copy(src, dst,
            boost::filesystem::copy_options::recursive |
            boost::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("failed to copy directory: " + src.string(), ec);
    }
}

/**
 * @brief carries the blob directory over from the original log directory to the working directory.
 *
 * Without this step the blob data would be lost when from_dir is removed (or renamed away for
 * backup) and replaced by the working directory. When make_backup is requested the blob
 * directory is copied so that the backup keeps its own blob data; otherwise it is moved by
 * rename. The move requires from_dir and tmp to be on the same filesystem; compaction()
 * already rejects a cross-filesystem working directory up front, so the rename below is a
 * defense-in-depth check that should not normally fail.
 * @param from_dir the original log directory
 * @param tmp the working directory the compacted log is assembled in
 * @param make_backup true if from_dir is kept as a backup, so blob data must remain in it
 */
void carry_over_blob_directory(
        boost::filesystem::path const& from_dir, boost::filesystem::path const& tmp,
        bool make_backup) {
    boost::filesystem::path src = from_dir / "blob";
    boost::system::error_code ec;
    bool present = boost::filesystem::exists(src, ec);
    if (ec && ec != boost::system::errc::no_such_file_or_directory) {
        LOG_AND_THROW_IO_EXCEPTION(
                "failed to check existence of blob directory: " + src.string(), ec);
    }
    if (!present) {
        return;  // no blob data; nothing to carry over
    }
    boost::filesystem::path dst = tmp / "blob";
    if (make_backup) {
        VLOG_LP(log_info) << "copying blob directory " << src << " to " << dst;
        copy_directory_recursively(src, dst);
        return;
    }
    VLOG_LP(log_info) << "moving blob directory " << src << " to " << dst;
    boost::filesystem::rename(src, dst, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION(
                "failed to move blob directory (the working directory must be on the same "
                "filesystem as the dblogdir): " + src.string(), ec);
    }
}

void compaction(dblog_scan &ds) {
    // The --epoch option is not meaningful for compaction: it never restricts the
    // compacted data, so it is ignored here (consistent with other options that do
    // not apply to a given subcommand). The durable epoch recorded in the log
    // directory is always used.
    epoch_id_type ld_epoch{};
    try {
        ld_epoch = ds.last_durable_epoch_in_dir();
    } catch (limestone_exception& ex) {
        LOG(ERROR) << "reading epoch file is failed: " << ex.what();
        log_and_exit(64);
    }
    std::cout << "durable-epoch: " << ld_epoch << std::endl;
    auto from_dir = ds.get_dblogdir();
    {
        auto p = from_dir;  // make copy
        remove_trailing_dir_separators(p);
        if (boost::filesystem::is_symlink(p)) {
            LOG(ERROR) << "dblogdir is symlink; compaction target must not be symlink";
            log_and_exit(64);
        }
    }
    boost::filesystem::path tmp;
    if (!FLAGS_working_dir.empty()) {
        tmp = FLAGS_working_dir;
        // TODO: check, error if exist and non-empty
    } else {
        tmp = make_work_dir_next_to(from_dir);
    }
    std::cout << "working-directory: " << tmp << std::endl;

    // The working directory must be on the same filesystem as the dblogdir. Compaction
    // carries the log directory contents over into tmp and finally renames tmp onto
    // from_dir; both the blob move and that final rename fail across filesystems. With
    // --make_backup this would be especially harmful: from_dir is first renamed away to
    // the backup, and only then does rename(tmp, from_dir) fail, leaving no from_dir
    // restored. Reject a cross-filesystem working directory up front, before any
    // destructive step, regardless of --make_backup.
    if (!on_same_filesystem(from_dir, tmp)) {
        LOG(ERROR) << "working directory must be on the same filesystem as the dblogdir: "
                   << "dblogdir=" << from_dir << ", working-directory=" << tmp;
        log_and_exit(64);
    }

    if (!FLAGS_force) {
        // prompt
        char yn = 'N';
        std::cout << "execute? (y/N) ";
        std::cin >> yn;
        if (yn != 'y' && yn != 'Y') {
            LOG(ERROR) << "aborted";
            log_and_exit(0);
        }
    }

    setup_initial_logdir(tmp);

    // Carry over the original manifest so that the instance_uuid is preserved
    // (see the offline compaction blob-loss issue).
    carry_over_manifest_file(from_dir, tmp);

    VLOG_LP(log_info) << "making compact pwal file to " << tmp;
    compaction_options options{from_dir, tmp, FLAGS_thread_num};
    blob_id_type max_blob_id = create_compact_pwal_and_get_max_blob_id(options);

    // epoch file
    VLOG_LP(log_info) << "making compact epoch file to " << tmp;
    FILE* strm = fopen((tmp / "epoch").c_str(), "a");  // NOLINT(*-owning-memory)
    if (!strm) {
        LOG_AND_THROW_IO_EXCEPTION("fopen failed", errno);
    }
    // TODO: if to-flat mode, set ld_epoch := 1
    log_entry::durable_epoch(strm, ld_epoch);
    if (fflush(strm) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fflush failed", errno);
    }
    if (fsync(fileno(strm)) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fsync failed", errno);
    }
    if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("fclose failed", errno);
    }

    // Update the compaction catalog so that the compacted file is registered.
    // Without this, a subsequent startup treats the directory as if no compaction
    // had been performed, and remove entries are dropped from the snapshot,
    // resurrecting deleted records (see tsurugi-issues #1498).
    carry_over_and_update_compaction_catalog(from_dir, tmp, ld_epoch, max_blob_id);

    if (FLAGS_dry_run) {
        std::cout << "compaction will be successfully completed (dry-run mode)" << std::endl;
        VLOG_LP(log_info) << "deleting work directory " << tmp;
        boost::filesystem::remove_all(tmp);
        return;
    }

    // Carry over the blob data after the dry-run check so that a dry run never
    // touches from_dir, and right before the directory swap below.
    carry_over_blob_directory(from_dir, tmp, FLAGS_make_backup);

    if (FLAGS_make_backup) {
        auto bkdir = make_backup_dir_next_to(from_dir);
        VLOG_LP(log_info) << "renaming " << from_dir << " to " << bkdir << " for backup";
        boost::filesystem::rename(from_dir, bkdir);
        // Report the backup destination so the user can locate it; the path is
        // generated internally and otherwise only visible in verbose logs.
        std::cout << "backup-directory: " << bkdir << std::endl;
    } else {
        VLOG_LP(log_info) << "deleting " << from_dir;
        boost::filesystem::remove_all(from_dir);
    }
    VLOG_LP(log_info) << "renaming " << tmp << " to " << from_dir;
    boost::filesystem::rename(tmp, from_dir);

    std::cout << "compaction was successfully completed: " << from_dir << std::endl;
}

}

namespace limestone {

int main(char *dir, subcommand mode) {  // NOLINT
    if (FLAGS_verbose) {
        if (FLAGS_v < log_debug) { // NOLINT(readability-use-std-min-max)
            FLAGS_v = log_debug;
        }
    }
    // --epoch is used only by the inspect and repair subcommands. For other
    // subcommands it is ignored without even being validated, so that options
    // belonging to other subcommands never affect the current one.
    bool const uses_epoch = (mode == cmd_inspect || mode == cmd_repair);
    std::optional<epoch_id_type> opt_epoch;
    if (!uses_epoch || FLAGS_epoch.empty()) {
        opt_epoch = std::nullopt;
    } else {
        std::size_t idx{};
        bool error = false;
        try {
            opt_epoch = std::stoul(FLAGS_epoch, &idx);
        } catch (std::exception& e) {
            error = true;
        }
        if (error || FLAGS_epoch[idx] != '\0') {
            LOG(ERROR) << "invalid value for --epoch option";
            log_and_exit(64);
        }
    }
    boost::filesystem::path p(dir);
    std::cout << "dblogdir: " << p << std::endl;
    if (!boost::filesystem::exists(p)) {
        LOG(ERROR) << "dblogdir not exists";
        log_and_exit(64);
    }
    try {
        int lock_fd = manifest::acquire_lock(p);
        check_and_migrate_logdir_format(p);
        if (lock_fd == -1) {
            LOG(ERROR) << "Log directory " << p
                       << " is already in use by another process. Operation aborted.";
            log_and_exit(64);
        }
        dblog_scan ds(p);
        ds.set_thread_num(FLAGS_thread_num);
        if (mode == cmd_inspect) inspect(ds, opt_epoch);
        if (mode == cmd_repair) repair(ds, opt_epoch);
        if (mode == cmd_compaction) compaction(ds);
        close(lock_fd);
    } catch (limestone_exception& e) {
        LOG(ERROR) << e.what();
        log_and_exit(64);
    }
    return 0;
}

}

int main(int argc, char *argv[]) {  // NOLINT
    gflags::SetUsageMessage("Tsurugi dblog maintenance command\n\n"
                            //"usage: tglogutil {inspect | repair | compaction} [options] <dblogdir>"
                            "usage: tglogutil {repair | compaction} [options] <dblogdir>"
                            );
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const char *arg0 = argv[0];  // NOLINT(*-pointer-arithmetic)
    google::InitGoogleLogging(arg0);
    subcommand mode{};
    auto usage = [&arg0]() {
        //std::cout << "usage: " << arg0 << " {inspect | repair | compaction} [options] <dblogdir>" << std::endl;
        std::cout << "usage: " << arg0 << " {repair | compaction} [options] <dblogdir>" << std::endl;
        log_and_exit(1);
    };
    if (FLAGS_h) {
        gflags::ShowUsageWithFlags(arg0);
        exit(1);
    }
    if (argc < 3) {
        LOG(ERROR) << "missing parameters";
        usage();
    }
    const char *arg1 = argv[1];  // NOLINT(*-pointer-arithmetic)
    if (strcmp(arg1, "inspect") == 0) {
        LOG(WARNING) << "WARNING: subcommand 'inspect' is under development";
        mode = cmd_inspect;
    } else if (strcmp(arg1, "repair") == 0) {
        mode = cmd_repair;
    } else if (strcmp(arg1, "compaction") == 0) {
        mode = cmd_compaction;
    } else {
        LOG(ERROR) << "unknown subcommand: " << arg1;
        usage();
    }
    return limestone::main(argv[2], mode);  // NOLINT(*-pointer-arithmetic)
}
// NOLINTEND(performance-avoid-endl)