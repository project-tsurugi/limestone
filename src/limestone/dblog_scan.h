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

#include <boost/filesystem.hpp>

#include <limestone/api/datastore.h>
#include "internal.h"
#include "log_entry.h"

namespace limestone::internal {
// accessing dblogdir before db start
class dblog_scan {

    // XXX: copied from datastore.h, resolve dup
    /**
     * @brief name of a file to record durable epoch
     */
    static constexpr const std::string_view epoch_file_name = "epoch";  /* datastore::epoch_file_name */

    // XXX: copied from log_channel.h, resolve dup
    /**
     * @brief prefix of pwal file name
     */
    static constexpr const std::string_view pwal_prefix = "pwal_";  /* log_channel::prefix */

public:
    class parse_error {
    public:
        enum code {
            ok = 0,
            repaired = 1,
            // warning; pending repair (cut) (inner-code, do not expose out of parse-func)
            broken_after_tobe_cut = 0x8,
            // warning; repaired, but tail is still broken, so do not append to this file
            broken_after_marked = 0x11,
            // error; nondurable snippets exist
            nondurable_entries = 0x40,
            // error; tail is broken, not repaired
            broken_after = 0x41,
            // error; unexpected (formal) entry, maybe logic error
            unexpected = 0x81,
            failed = 0xff,
        };

        parse_error() noexcept : value_(ok) {}
        explicit parse_error(code value) noexcept : value_(value) {}
        parse_error(code value, std::streamoff fpos) noexcept : value_(value), fpos_(fpos) {}

        void value(code value) noexcept { value_ = value; }
        [[nodiscard]] code value() const noexcept { return value_; }
        explicit operator bool() const noexcept { return value_ != ok; }

        [[nodiscard]] std::streamoff fpos() const noexcept { return fpos_; }

        [[nodiscard]] std::string message() const {
            switch (value_) {
            case ok: return "OK";
            case repaired: return "file is repaired";
            case broken_after_tobe_cut: return "file is broken after offset " + std::to_string(fpos_) + ", and pending to cut";
            case broken_after_marked: return "file is broken after offset " + std::to_string(fpos_) + ", and marked invalid snippet";
            case nondurable_entries: return "nondurable entries remain";
            case broken_after: return "file is broken after offset " + std::to_string(fpos_) + ", need to be repair";
            case unexpected: return "unexpected log entry order";
            case failed: return "parse failed";
            }
            return "unknown error code " + std::to_string(value_);
        }
    private:
        code value_;
        std::streamoff fpos_{-1};
    };

    using error_report_func_t = std::function<bool(log_entry::read_error&)>;

    explicit dblog_scan(const boost::filesystem::path& logdir) : dblogdir_(logdir) { }
    explicit dblog_scan(boost::filesystem::path&& logdir) : dblogdir_(std::move(logdir)) { }

    void set_thread_num(int thread_num) noexcept { thread_num_ = thread_num; }
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
    epoch_id_type scan_pwal_files_throws(epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry);
    /**
     * @returns max epoch value in directory
     */
    epoch_id_type scan_pwal_files(epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry,
        const error_report_func_t& report_error, dblog_scan::parse_error::code* max_parse_error_value = nullptr);

    epoch_id_type scan_one_pwal_file(const boost::filesystem::path& p, epoch_id_type ld_epoch,
        const std::function<void(log_entry&)>& add_entry,
        const error_report_func_t& report_error,
        parse_error& pe);

    static bool is_wal(const boost::filesystem::path& p) { return p.filename().string().rfind(pwal_prefix, 0) == 0; }
    static bool is_detached_wal(const boost::filesystem::path& p) {
        auto filename = p.filename().string();
        return (filename.length() > 9 && filename.rfind(pwal_prefix, 0) == 0);
    }

private:
    boost::filesystem::path dblogdir_;
    int thread_num_{1};
    bool fail_fast_{false};

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
