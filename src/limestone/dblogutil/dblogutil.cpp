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

void log_and_exit(int error) {
    VLOG(10) << "exiting with code " << error;
    exit(error);
}

namespace limestone {

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
    case dblog_scan::parse_error::unexpected:
        std::cout << "status: unrepairable" << std::endl;
        log_and_exit(16);
    case dblog_scan::parse_error::failed:
        std::cout << "status: cannot-check" << std::endl;
        log_and_exit(64);
    }
}

static boost::filesystem::path make_work_dir_next_to(const boost::filesystem::path& target_dir) {
    // assume: already checked existence and is_dir
    return make_tmp_dir_next_to(target_dir, ".work_XXXXXX");
}

static boost::filesystem::path make_backup_dir_next_to(const boost::filesystem::path& target_dir) {
    return make_tmp_dir_next_to(target_dir, ".backup_XXXXXX");
}

void compaction(dblog_scan &ds, std::optional<epoch_id_type> epoch) {
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

    VLOG_LP(log_info) << "making compact pwal file to " << tmp;
    create_compact_pwal_and_get_max_blob_id(from_dir, tmp, FLAGS_thread_num, std::set<std::string>{});

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

    if (FLAGS_dry_run) {
        std::cout << "compaction will be successfully completed (dry-run mode)" << std::endl;
        VLOG_LP(log_info) << "deleting work directory " << tmp;
        boost::filesystem::remove_all(tmp);
        return;
    }

    if (FLAGS_make_backup) {
        auto bkdir = make_backup_dir_next_to(from_dir);
        VLOG_LP(log_info) << "renaming " << from_dir << " to " << bkdir << " for backup";
        boost::filesystem::rename(from_dir, bkdir);
    } else {
        VLOG_LP(log_info) << "deleting " << from_dir;
        boost::filesystem::remove_all(from_dir);
    }
    VLOG_LP(log_info) << "renaming " << tmp << " to " << from_dir;
    boost::filesystem::rename(tmp, from_dir);

    std::cout << "compaction was successfully completed: " << from_dir << std::endl;
}

int main(char *dir, subcommand mode) {  // NOLINT
    if (FLAGS_verbose) {
        if (FLAGS_v < log_info) {
            FLAGS_v = log_info;
        }
    }
    std::optional<epoch_id_type> opt_epoch;
    if (FLAGS_epoch.empty()) {
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
        check_and_migrate_logdir_format(p);
        int lock_fd = acquire_manifest_lock(p);
        if (lock_fd == -1) {
            LOG(ERROR) << "Log directory " << p
                       << " is already in use by another process. Operation aborted.";
            log_and_exit(64);
        }
        dblog_scan ds(p);
        ds.set_thread_num(FLAGS_thread_num);
        if (mode == cmd_inspect) inspect(ds, opt_epoch);
        if (mode == cmd_repair) repair(ds, opt_epoch);
        if (mode == cmd_compaction) compaction(ds, opt_epoch);
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
