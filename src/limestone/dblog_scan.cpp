/*
 * Copyright 2024-2026 Project Tsurugi.
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

#include <cassert>
#include <iomanip>
#include <map>
#include <mutex>
#include <set>
#include <utility>
#include <boost/filesystem.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"

#include <limestone/api/datastore.h>
#include "internal.h"
#include "dblog_scan.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_reader.h"

namespace {
using namespace limestone;
using namespace limestone::api;

bool log_error_and_throw(log_entry::read_error& e) {
    LOG_AND_THROW_EXCEPTION("this pwal file is broken: " + e.message());
    return false;
}

}




namespace limestone::internal {
using namespace limestone::api;


// return max epoch in file.
std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file) {
    std::optional<epoch_id_type> rv;

    // ASSERT: file exists
    log_entry_reader reader(file);
    while (true) {
        const auto read_result = reader.next();
        if (read_result.result() == log_entry_read_result::status::end) {
            break;
        }
        if (read_result.result() == log_entry_read_result::status::error) {
            LOG_AND_THROW_EXCEPTION("this epoch file is broken: " + file.string() + " at offset "
                                    + std::to_string(read_result.offset()) + ": " + read_result.error().message());
        }
        const auto entry = read_result.view();
        if (entry.type() != log_entry::entry_type::marker_durable) {
            LOG_AND_THROW_EXCEPTION("this epoch file is broken: " + file.string() + " at offset "
                                    + std::to_string(read_result.offset()) + ": unexpected log_entry type: "
                                    + std::to_string(static_cast<int>(entry.type())));
        }
        if (!rv.has_value() || entry.epoch_id() > rv) {
            rv = entry.epoch_id();
        }
    }
    return rv;
}



epoch_id_type dblog_scan::last_durable_epoch_in_dir() {
    auto& from_dir = dblogdir_;
    // read main epoch file first
    auto main_epoch_file = from_dir / std::string(epoch_file_name);
    
    // If main epoch file does not exist, create an empty one
    if (!boost::filesystem::exists(main_epoch_file)) {
        std::ofstream(main_epoch_file.string()).close();  // Create an empty file
    } else {
        // If the file exists, attempt to get the last durable epoch
        std::optional<epoch_id_type> ld_epoch = last_durable_epoch(main_epoch_file);
        if (ld_epoch.has_value()) {
            return *ld_epoch;
        }
    }

    // main epoch file is empty or does not contain a valid epoch,
    // read all rotated-epoch files
    std::optional<epoch_id_type> ld_epoch;
    auto epoch_files = filter_epoch_files(from_dir);
    for (const boost::filesystem::path& p : epoch_files) {
        std::optional<epoch_id_type> epoch = last_durable_epoch(p);
        if (!epoch.has_value()) {
            continue;  // file is empty
        }
        if (!ld_epoch.has_value() || *ld_epoch < *epoch) {
            ld_epoch = epoch;
        }
    }
    return ld_epoch.value_or(0);  // 0 = minimum epoch
}



void dblog_scan::detach_wal_files(bool skip_empty_files) {
    // rotate_attached_wal_files
    std::vector<boost::filesystem::path> attached_files;
    for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(dblogdir_)) {
        if (is_wal(p) && !is_detached_wal(p)) {
            if (skip_empty_files && boost::filesystem::is_empty(p)) {
                continue;
            }
            attached_files.emplace_back(p);
        }
    }
    if (attached_files.empty()) {
        return;
    }
    for (const boost::filesystem::path& p : attached_files) {
        std::stringstream ssbase;
        auto unix_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        ssbase << p.string() << "." << std::setw(14) << std::setfill('0') << unix_epoch << ".";
        std::string base = ssbase.str();
        for (int suffix = 0; ; suffix++) {
            boost::filesystem::path new_file{base + std::to_string(suffix)};
            if (!boost::filesystem::exists(new_file)) {
                boost::filesystem::rename(p, new_file);
                VLOG_LP(50) << "rename " << p << " to " << new_file;
                break;
            }
        }
    }
    // persist the result of the renames
    real_file_operations default_ops;
    file_operations& ops = file_ops_ != nullptr ? *file_ops_ : default_ops;
    if (ops.fsync_directory(dblogdir_) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to fsync directory: " + dblogdir_.string(), errno);
    }
}

void dblog_scan::set_file_operations(file_operations& ops) noexcept {
    file_ops_ = &ops;
}

epoch_id_type dblog_scan::scan_pwal_files(epoch_id_type ld_epoch, const add_entry_func_t& add_entry,
                                          const error_report_func_t& report_error,
                                          dblog_scan::parse_error::code* max_parse_error_value) {
    input_file_queue queue;
    for (const auto& p : path_list_) {
        queue.push(p);
    }
    queue.close();
    return scan_pwal_files(queue, ld_epoch, add_entry, report_error, max_parse_error_value);
}

epoch_id_type dblog_scan::scan_pwal_files(  // NOLINT(readability-function-cognitive-complexity)
        input_file_queue& queue, epoch_id_type ld_epoch, const add_entry_func_t& add_entry,
        const error_report_func_t& report_error, dblog_scan::parse_error::code* max_parse_error_value) {
    std::atomic<epoch_id_type> max_appeared_epoch{ld_epoch};
    if (max_parse_error_value) { *max_parse_error_value = dblog_scan::parse_error::failed; }
    std::atomic<dblog_scan::parse_error::code> max_error_value{dblog_scan::parse_error::code::ok};
    std::map<boost::filesystem::path, epoch_id_type> max_epoch_per_file;
    std::mutex max_epoch_per_file_mtx;
    std::atomic<blob_id_type> max_blob_id_in_dir{0};
    // NOLINTNEXTLINE(readability-function-cognitive-complexity)
    auto process_file = [&](const scan_input& input, int worker) {
        const boost::filesystem::path& p = input.path();
        if (is_wal(p)) {
            parse_error ec;
            const std::function<void(log_entry&)> add_entry_of_worker = [&add_entry, worker](log_entry& e) {
                add_entry(e, worker);
            };
            const file_scan_result rc = scan_one_pwal_file(input, ld_epoch, add_entry_of_worker, report_error, ec);
            epoch_id_type max_epoch_of_file = rc.max_epoch;
            auto ec_value = ec.value();
            switch (ec_value) {
            case parse_error::ok:
                VLOG(log_debug) << "OK: " << p;
                break;
            case parse_error::repaired:
                VLOG(log_debug) << "REPAIRED: " << p;
                break;
            case parse_error::broken_after_marked:
                if (!is_detached_wal(p)) {
                    VLOG(log_debug) << "MARKED BUT TAIL IS BROKEN (NOT DETACHED): " << p;
                    if (fail_fast_) {
                        THROW_LIMESTONE_EXCEPTION("the end of non-detached file is broken");
                    }
                } else {
                    VLOG(log_debug) << "MARKED BUT TAIL IS BROKEN (DETACHED): " << p;
                    ec.value(ec.modified() ? parse_error::repaired : parse_error::ok);
                }
                break;
            case parse_error::broken_after:
                VLOG(log_debug) << "TAIL IS BROKEN: " << p;
                if (!is_detached_wal(p)) {
                    if (fail_fast_) {
                        THROW_LIMESTONE_EXCEPTION("the end of non-detached file is broken");
                    }
                }
                break;
            case parse_error::nondurable_entries:
                VLOG(log_debug) << "CONTAINS NONDURABLE ENTRY: " << p;
                break;
            case parse_error::corrupted_durable_entries:
                VLOG(log_debug) << "DURABLE EPOCH ENTRIES ARE CORRUPTED: " << p;
                if (fail_fast_) {
                    THROW_LIMESTONE_EXCEPTION(ec.message());
                }
                break;
            case parse_error::unexpected:
            case parse_error::failed:
                VLOG(log_debug) << "ERROR: " << p;
                if (fail_fast_) {
                    THROW_LIMESTONE_EXCEPTION(ec.message());
                }
                break;
            case parse_error::broken_after_tobe_cut: assert(false);
            }
            auto tmp = max_error_value.load();
            while (tmp < ec.value()
                   && !max_error_value.compare_exchange_weak(tmp, ec.value())) {
                /* nop */
            }
            epoch_id_type t = max_appeared_epoch.load();
            while (t < max_epoch_of_file
                   && !max_appeared_epoch.compare_exchange_weak(t, max_epoch_of_file)) {
                /* nop */
            }
            blob_id_type b = max_blob_id_in_dir.load();
            while (b < rc.max_blob_id
                   && !max_blob_id_in_dir.compare_exchange_weak(b, rc.max_blob_id)) {
                /* nop */
            }
            {
                std::scoped_lock<std::mutex> lock(max_epoch_per_file_mtx);
                max_epoch_per_file[p] = max_epoch_of_file;
            }
        }
    };

    std::mutex ex_mtx;
    std::exception_ptr ex_ptr{};

    std::vector<std::thread> workers;
    workers.reserve(thread_num_);
    for (int i = 0; i < thread_num_; i++) {
        workers.emplace_back([&, i](){
            try {
                while (const auto p = queue.pop()) {
                    process_file(*p, i);
                    if (options_.has_value()) {
                        compaction_options &opts = options_.value().get();
                        if (opts.is_gc_enabled()) {
                            opts.get_gc_snapshot().finalize_local_entries();
                        }
                    }
                }
                // the hook is called only when the input is exhausted (not when stopped by another thread's failure)
                if (!queue.aborted() && thread_end_hook_) {
                    thread_end_hook_(i);
                }
            } catch (limestone_exception& ex) {
                VLOG(log_info) << "/:limestone catch runtime_error(" << ex.what() << ")";
                {
                    std::scoped_lock<std::mutex> lock(ex_mtx);
                    if (!ex_ptr) {  // only save one
                        ex_ptr = std::current_exception();
                    }
                }
                queue.abort();  // stops the other workers
            }
        });
    }
    for (int i = 0; i < thread_num_; i++) {
        workers[i].join();
    }
    if (ex_ptr) {
        std::rethrow_exception(ex_ptr);
    }
    if (queue.aborted()) {  // aborted by the producer
        THROW_LIMESTONE_EXCEPTION("the scan is aborted");
    }
    max_epoch_per_file_ = std::move(max_epoch_per_file);
    max_blob_id_ = max_blob_id_in_dir.load();
    if (max_parse_error_value) { *max_parse_error_value = max_error_value; }
    return max_appeared_epoch;
}

// called from datastore::create_snapshot
// db_startup mode
epoch_id_type dblog_scan::scan_pwal_files_throws(epoch_id_type ld_epoch, const add_entry_func_t& add_entry) {
    set_fail_fast(true);
    set_process_at_nondurable_epoch_snippet(process_at_nondurable::repair_by_mark);
    set_process_at_truncated_epoch_snippet(process_at_truncated::report);
    set_process_at_damaged_epoch_snippet(process_at_damaged::report);
    return scan_pwal_files(ld_epoch, add_entry, log_error_and_throw);
}

// called from the online compaction path
// Snippets beyond the boundary are skipped in the ignore mode (files stay unmodified).
// The parser treats such a snippet as valid = false, so none of its entries reaches
// add_entry.
epoch_id_type dblog_scan::scan_pwal_files_for_compaction(epoch_id_type boundary_epoch,
                                                          const add_entry_func_t& add_entry) {
    set_fail_fast(true);
    set_process_at_nondurable_epoch_snippet(process_at_nondurable::ignore);
    set_process_at_truncated_epoch_snippet(process_at_truncated::report);
    set_process_at_damaged_epoch_snippet(process_at_damaged::report);
    return scan_pwal_files(boundary_epoch, add_entry, log_error_and_throw);
}

void dblog_scan::set_thread_end_hook(thread_end_hook_t hook) {
    thread_end_hook_ = std::move(hook);
}

void dblog_scan::rescan_directory_paths() {
    path_list_.clear();
    if (boost::filesystem::exists(dblogdir_) && boost::filesystem::is_directory(dblogdir_)) {
        for (boost::filesystem::directory_iterator it(dblogdir_), end; it != end; ++it) {
            path_list_.push_back(it->path());
        }
    }
}

} // namespace limestone::internal
