/*
 * Copyright 2024-2025 Project Tsurugi.
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
#pragma once

#include <boost/filesystem.hpp>
#include <list>
#include <map>

#include <limestone/api/datastore.h>
#include "internal.h"
#include "log_entry.h"
#include "compaction_options.h"
#include "input_file_queue.h"
#include "scan_input.h"


namespace limestone::internal {
// accessing dblogdir before db start
class dblog_scan {

public:
    class parse_error {
    public:
        enum code {
            ok = 0,                         // ok; no error at all
            repaired = 1,                   // ok; repaired
            broken_after_tobe_cut = 0x8,    // warning; pending repair (cut) (inner-code, do not expose out of parse-func)
            broken_after_marked = 0x11,     // warning; repaired, but tail is still broken, so do not append to this file
            nondurable_entries = 0x40,      // error; nondurable (formal (not short) and alive (not invalidated)) snippets exist, so need to mark invalidated
            broken_after = 0x41,            // error; tail is broken, not repaired yet (but can repair)
            corrupted_durable_entries = 0x42, // fatal; durable entries physically corrupted and unrecoverable
            unexpected = 0x81,              // error; unexpected (formal) entry, maybe logic error
            failed = 0xff,
        };

        parse_error() noexcept : value_(ok) {}
        explicit parse_error(code value) noexcept : value_(value) {}
        parse_error(code value, std::streamoff fpos) noexcept : value_(value), fpos_(fpos) {}

        void value(code value) noexcept { value_ = value; }
        [[nodiscard]] code value() const noexcept { return value_; }
        explicit operator bool() const noexcept { return value_ != ok; }
        void modified(bool modified) noexcept { modified_ = modified; }
        [[nodiscard]] bool modified() const noexcept { return modified_; }

        [[nodiscard]] std::streamoff fpos() const noexcept { return fpos_; }

        [[nodiscard]] std::string message() const {
            switch (value_) {
            case ok: return "OK";
            case repaired: return "file is repaired";
            case broken_after_tobe_cut: return "file is broken after offset " + std::to_string(fpos_) + ", and pending to cut";
            case broken_after_marked: return "file is broken after offset " + std::to_string(fpos_) + ", and marked invalid snippet";
            case nondurable_entries: return "nondurable entries remain";
            case broken_after: return "file is broken after offset " + std::to_string(fpos_) + ", need to be repair";
            case corrupted_durable_entries: return "durable epoch snippet is physically corrupted and unrecoverable";
            case unexpected: return "unexpected log entry order";
            case failed: return "parse failed";
            }
            return "unknown error code " + std::to_string(value_);
        }
    private:
        code value_;
        bool modified_{false};
        std::streamoff fpos_{-1};
    };

    using error_report_func_t = std::function<bool(log_entry::read_error&)>;

    /// the type of add_entry of the scan_pwal_files family. Receives an entry and the number of the worker that read
    /// it (0 to thread_num - 1)
    using add_entry_func_t = std::function<void(log_entry&, int)>;

    /// the type of the thread end hook. Receives the number of the worker (the same as the one passed to add_entry)
    using thread_end_hook_t = std::function<void(int)>;

    /**
     * @brief Constructor that initializes the dblog_scan with the specified log directory.
     *
     * This constructor uses the initial content of the `logdir` to determine the target files for processing.
     * If the contents of `logdir` change during processing, you must call the `rescan_directory_paths()` method
     * to update the target files accordingly.
     *
     * @param logdir The path to the directory containing the files to be processed.
     */
     explicit dblog_scan(boost::filesystem::path logdir) : dblogdir_(std::move(logdir)) {
        rescan_directory_paths();
    }


    /**
     * @brief Constructor that initializes the dblog_scan with the specified log directory and file names.
     *
     * This constructor processes only the files specified in the `file_names` set within the `logdir`.
     * It is used when you do not want the target files to change even if the contents of `logdir` are modified after
     * determining the target files.
     *
     * @param logdir The path to the directory containing the files to be processed.
     * @param file_names The set of file names within `logdir` to be processed.
     */
     explicit dblog_scan(boost::filesystem::path logdir, compaction_options& options) 
     : dblogdir_(std::move(logdir)), options_(options) {
         for (const auto& file_name : options.get_file_names()) {
             path_list_.emplace_back(dblogdir_ / file_name);
         }
     }

    const boost::filesystem::path& get_dblogdir() { return dblogdir_; }
    void set_thread_num(int thread_num) noexcept { thread_num_ = thread_num; }

    /**
     * @brief Sets the function called once in each worker thread when the thread finishes after exhausting the
     *        input.
     *
     * In a scan of the scan_pwal_files family, each worker calls it with its worker number after no file is left to
     * take and before the thread ends. It is not called when the scan stops by a failure (an exception in the
     * thread itself, or the thread stopping on seeing the failure of another thread). However, a thread that
     * exhausted the input before the failure of another thread calls it.
     * A limestone_exception thrown by the hook is rethrown from the scan_pwal_files family as a failure of the scan.
     * @param hook the function to call. Not called if empty (the default)
     */
    void set_thread_end_hook(thread_end_hook_t hook);
    void set_fail_fast(bool fail_fast) noexcept { fail_fast_ = fail_fast; }
    void detach_wal_files(bool skip_empty_files = true);

    enum class process_at_nondurable {
        ignore,
        report,
        repair_by_mark,  // mark invalidated epoch snippet header of non-durable well-fromed epoch snippet
        //repair_by_cut,  // cut the snippet from the file
    };
    void set_process_at_nondurable_epoch_snippet(process_at_nondurable p) noexcept {
        process_at_nondurable_ = p;
    }

    enum class process_at_truncated {
        ignore,
        report,
        repair_by_mark,  // mark invalidated to epoch snippet header of incomplete (truncated) epoch snippet
        repair_by_cut,  // truncate the incomplete snippet from the file
    };
    void set_process_at_truncated_epoch_snippet(process_at_truncated p) noexcept {
        process_at_truncated_ = p;
    }

    enum class process_at_damaged {
        ignore,
        report,
        repair_by_mark,  // mark invalidated to epoch snippet header of broken epoch snippet (contains log entry which type is unknown)
        repair_by_cut,  // remove the damaged snippet from snippet header to the end of file
    };
    void set_process_at_damaged_epoch_snippet(process_at_damaged p) noexcept {
        process_at_damaged_ = p;
    }

    epoch_id_type last_durable_epoch_in_dir();

    /**
     * @returns max epoch value in directory
     * @throws exception on error
     */
    epoch_id_type scan_pwal_files_throws(epoch_id_type ld_epoch, const add_entry_func_t& add_entry);

    /**
     * @brief scan for the compaction path: snippets beyond the boundary epoch are
     *        skipped, never invalidated (input files are not modified)
     * @param boundary_epoch the compaction boundary epoch; only entries at or below it reach add_entry
     * @param add_entry callback that receives the entries at or below the boundary
     * @returns max epoch in the directory, including the snippets beyond the boundary
     * @throws exception on error
     */
    epoch_id_type scan_pwal_files_for_compaction(epoch_id_type boundary_epoch, const add_entry_func_t& add_entry);

    /**
     * @brief returns the per-file max epoch recorded by the last successful scan
     * @return the keys are every file the scan processed (files satisfying is_wal,
     *         which includes non-WAL files whose name starts with pwal, such as
     *         pwal_0000.compacted)
     */
    [[nodiscard]] const std::map<boost::filesystem::path, epoch_id_type>& get_max_epoch_per_file() const noexcept {
        return max_epoch_per_file_;
    }
    /**
     * @brief Scans the files determined at construction.
     *
     * The same as calling the scan_pwal_files that takes a queue, with a queue closed after all the files are
     * pushed.
     * @returns max epoch value in directory
     */
    epoch_id_type scan_pwal_files(epoch_id_type ld_epoch, const add_entry_func_t& add_entry,
        const error_report_func_t& report_error, dblog_scan::parse_error::code* max_parse_error_value = nullptr);

    /**
     * @brief Scans the files taken out of a queue.
     *
     * thread_num workers take the paths out of the queue one by one and scan them. A worker waits if the queue is
     * empty and not closed. Once the queue is closed and empty, each worker calls the thread end hook and ends.
     * A failure of a worker (limestone_exception) aborts the queue to stop the other workers, and is rethrown
     * after all the workers end. If the producer aborts the queue, the workers stop reading the files they are
     * on, and the scan fails with a limestone_exception after all the workers end.
     * Only the files whose names start with pwal (is_wal) are scanned.
     * @param queue the queue of the input files. The scan does not end until the producer closes it
     * @param ld_epoch the durable epoch (or the boundary epoch of the compaction)
     * @param add_entry called with each valid entry and the number of the worker that read it
     * @param report_error called for each parse error
     * @param max_parse_error_value if not null, receives the maximum parse error of the files
     * @returns max epoch value in the files
     */
    epoch_id_type scan_pwal_files(input_file_queue& queue, epoch_id_type ld_epoch, const add_entry_func_t& add_entry,
        const error_report_func_t& report_error, dblog_scan::parse_error::code* max_parse_error_value = nullptr);

    /**
     * @brief Scans the WAL file of one input.
     *
     * Once stopped() of the input becomes true, reading stops there (checked for every entry). In that case the
     * return value and pe are those of the part read.
     * @param input the input to scan
     * @param ld_epoch the durable epoch (or the boundary epoch of the compaction)
     * @param add_entry called with each valid entry
     * @param report_error called for each parse error
     * @param pe receives the parse error of the file
     * @returns max epoch value in the file
     */
    epoch_id_type scan_one_pwal_file(const scan_input& input, epoch_id_type ld_epoch,
        const std::function<void(log_entry&)>& add_entry,
        const error_report_func_t& report_error,
        parse_error& pe);

    static bool is_wal(const boost::filesystem::path& p) { return p.filename().string().rfind(log_channel_prefix, 0) == 0; }
    static bool is_detached_wal(const boost::filesystem::path& p) {
        auto filename = p.filename().string();
        return (filename.length() > 9 && filename.rfind(log_channel_prefix, 0) == 0);
    }

    /**
     * @brief Rescans the directory and updates the path list.
     *
     * This function clears the current path list and rescans the specified
     * directory, adding all file paths in the directory to the path list.
     */
    void rescan_directory_paths();

private:
    boost::filesystem::path dblogdir_;
    std::optional<std::reference_wrapper<compaction_options>> options_;
    std::list<boost::filesystem::path> path_list_;
    int thread_num_{1};
    thread_end_hook_t thread_end_hook_{};
    bool fail_fast_{false};

    // Per-file max epoch recorded by the last successful scan.
    std::map<boost::filesystem::path, epoch_id_type> max_epoch_per_file_;

    // repair-nondurable-epoch-snippet
    //   (implemented in 1.0.0 BETA2)
    //   repair: non-durable well-fromed epoch snippet
    process_at_nondurable process_at_nondurable_ = process_at_nondurable::report;

    // repair-truncated-epoch-snippet
    //   (implemented in 1.0.0 BETA4)
    //   repair: incomplete (truncated) epoch snippet
    process_at_truncated process_at_truncated_ = process_at_truncated::report;

    // repair-damaged-epoch-snippet
    //   (implemented in 1.0.0 BETA4)
    //   damaged epoch snippet (contains log entry which type is unknown), e.g. zero-filled
    process_at_damaged process_at_damaged_ = process_at_damaged::report;
};

}
