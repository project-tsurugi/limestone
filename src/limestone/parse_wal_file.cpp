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

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <optional>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"

#include <limestone/api/datastore.h>
#include "dblog_scan.h"
#include "log_entry.h"
#include "log_entry/log_entry_reader.h"

namespace {

using namespace limestone;
using namespace limestone::api;

/**
 * @brief Rewrites the marker at the head of an invalid epoch snippet to marker_invalidated_begin, and fsyncs.
 *
 * Opens the file for writing separately from reading, writes at the position, and closes it.
 * @param file the pwal file
 * @param fpos_head_of_epoch_snippet the position of the marker at the head of the snippet
 * @throws limestone_io_exception if the file cannot be opened, written, or fsynced (with errno)
 */
void invalidate_epoch_snippet(const boost::filesystem::path& file, std::streamoff fpos_head_of_epoch_snippet) {
    // throw if the marker cannot be rewritten: if scanning continued as repaired without the rewrite, a snippet that
    // should be invalid could be treated as valid data at a later startup, after the durable epoch has advanced
    const int fd = ::open(file.c_str(), O_WRONLY);  // NOLINT(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    if (fd < 0) {
        const int err = errno;
        LOG_AND_THROW_IO_EXCEPTION("cannot open pwal file: " + file.string(), err);
    }
    const char buf = static_cast<char>(log_entry::entry_type::marker_invalidated_begin);
    errno = 0;  // so that a stale errno is not reported if pwrite returns 0
    if (::pwrite(fd, &buf, sizeof(char), static_cast<off_t>(fpos_head_of_epoch_snippet)) != 1) {
        const int err = errno;
        ::close(fd);
        LOG_AND_THROW_IO_EXCEPTION("cannot write pwal file: " + file.string(), err);
    }
    if (::fsync(fd) != 0) {
        const int err = errno;
        ::close(fd);
        LOG_AND_THROW_IO_EXCEPTION("cannot fsync pwal file: " + file.string(), err);
    }
    ::close(fd);
}
} // namespace


namespace limestone::internal {
using namespace limestone::api;

// LOGFORMAT_v1 pWAL syntax

//  parser rule (naive, base idea)
//   pwal_file                     = wal_header epoch_snippets (EOF)
//   wal_header                    = (empty)
//   epoch_snippets                = epoch_snippet epoch_snippets
//                                 | (empty)
//   epoch_snippet                 = snippet_header log_entries snippet_footer
//   snippet_header                = marker_begin
//                                 | marker_invalidated_begin
//   log_entries                   = log_entry log_entries
//                                 | (empty)
//   log_entry                     = normal_entry
//                                 | normal_with_blob
//                                 | remove_entry
//                                 | clear_storage
//                                 | add_storage
//                                 | remove_storage
//   snippet_footer                = (empty)

//  parser rule (with error-handle)
//   pwal_file                     = wal_header epoch_snippets (EOF)
//   wal_header                    = (empty)
//   epoch_snippets                = epoch_snippet epoch_snippets
//                                 | (empty)
//   epoch_snippet                 = { head_pos := ... } snippet_header log_entries snippet_footer
//   snippet_header                = marker_begin             { max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } }
//                                 | marker_invalidated_begin { max-epoch := max(...); valid := false }
//                                 | SHORT_marker_begin       { error-truncated }  // TAIL
//                                 | SHORT_marker_inv_begin   { }  // TAIL
//                                 | UNKNOWN_TYPE_entry       { if (valid) error-broken-snippet-header }  // TAIL // use previous 'valid'
//   log_entries                   = log_entry log_entries
//                                 | (empty)
//   log_entry                     = normal_entry             { if (valid) process-entry }
//                                 | normal_with_blob         { if (valid) process-entry }
//                                 | remove_entry             { if (valid) process-entry }
//                                 | clear_storage            { if (valid) process-entry }
//                                 | add_storage              { if (valid) process-entry }
//                                 | remove_storage           { if (valid) process-entry }
//                                 | SHORT_normal_entry       { if (valid) error-truncated }  // TAIL
//                                 | SHORT_normal_with_blob   { if (valid) error-truncated }  // TAIL
//                                 | SHORT_remove_entry       { if (valid) error-truncated }  // TAIL
//                                 | SHORT_clear_storage      { if (valid) error-truncated }  // TAIL
//                                 | SHORT_add_storage        { if (valid) error-truncated }  // TAIL
//                                 | SHORT_remove_storage     { if (valid) error-truncated }  // TAIL
//                                 | UNKNOWN_TYPE_entry       { if (valid) error-damaged-entry }  // TAIL
//   snippet_footer                = (empty)

// lexer rule (see log_entry.h)
//   marker_begin                  = 0x02 epoch
//   marker_invalidated_begin      = 0x06 epoch
//   normal_entry                  = 0x01 key_length value_length storage_id key(key_length) write_version_major write_version_minor value(value_length)
//   normal_with_blog              = 0x0a key_length value_length storage_id key(key_length) write_version_major write_version_minor value(value_length) blob_ids
//   remove_entry                  = 0x05 key_length storage_id key(key_length) writer_version_major writer_version_minor
//   marker_durable                = 0x04 epoch
//   marker_end                    = 0x03 epoch
//   clear_storage                 = 0x07 storage_id write_version_major write_version_minor
//   add_storage                   = 0x08 storage_id write_version_major write_version_minor
//   remove_storage                = 0x09 storage_id write_version_major write_version_minor
//   epoch                         = int64le
//   key_length                    = int32le
//   value_length                  = int32le
//   storage_id                    = int64le
//   write_version_major           = int64le
//   write_version_minor           = int64le
//   SHORT_marker_begin            = 0x02 byte(0-7)
//   SHORT_marker_inv_begin        = 0x06 byte(0-7)
//   SHORT_normal_entry            = 0x01 key_length value_length storage_id key(key_length) write_version_major write_version_minor value(<value_length)
//                                 | 0x01 key_length value_length storage_id key(key_length) byte(0-15)
//                                 | 0x01 key_length value_length storage_id key(<key_length)
//                                 | 0x01 byte(0-15)
//   SHORT_normal_with_blob        = 0x0a key_length value_length storage_id key(key_length) write_version_major write_version_minor value(<value_length) blob_ids
//   SHORT_remove_entry            = 0x05 key_length storage_id key(key_length) byte(0-15)
//                                 | 0x05 key_length storage_id key(<key_length)
//                                 | 0x05 byte(0-11)
//   SHORT_marker_durable          = 0x04 byte(0-7)
//   SHORT_marker_end              = 0x03 byte(0-7)
//   SHORT_clear_storage           = 0x07 byte(0-23)
//   SHORT_add_storage             = 0x08 byte(0-23)
//   SHORT_remove_storage          = 0x09 byte(0-23)
//   UNKNOWN_TYPE_entry            = 0x00 byte(0-)
//                                 | 0x07-0xff byte(0-)
//   // marker_durable and marker_end are not used in pWAL file
//   // SHORT_*, UNKNOWN_* appears just before EOF
    class lex_token {
    public:
        enum class token_type {
            eof = 0,
            normal_entry = 1,
            marker_begin = 2,
            marker_end = 3,
            marker_durable = 4,
            remove_entry = 5,
            marker_invalidated_begin = 6,
            clear_storage = 7,
            add_storage = 8,
            remove_storage = 9,
            normal_with_blob = 10,
            SHORT_normal_entry = 101,
            SHORT_marker_begin = 102,
            SHORT_marker_end = 103,
            SHORT_marker_durable = 104,
            SHORT_remove_entry = 105,
            SHORT_marker_inv_begin = 106,
            SHORT_clear_storage = 107,
            SHORT_add_storage = 108,
            SHORT_remove_storage = 109,
            SHORT_normal_with_blob = 110,
            UNKNOWN_TYPE_entry = 1001,
        };

        explicit lex_token(const log_entry_read_result& read_result) {
            set(read_result);
        }
        void set(const log_entry_read_result& read_result) {
            if (read_result.result() != log_entry_read_result::status::error) {
                if (read_result.result() == log_entry_read_result::status::end) {
                    value_ = token_type::eof;
                } else switch (read_result.view().type()) {  // NOLINT(*braces-around-statements)
                case log_entry::entry_type::normal_entry:             value_ = token_type::normal_entry; break;
                case log_entry::entry_type::normal_with_blob:         value_ = token_type::normal_with_blob; break;
                case log_entry::entry_type::marker_begin:             value_ = token_type::marker_begin; break;
                case log_entry::entry_type::marker_end:               value_ = token_type::marker_end; break;
                case log_entry::entry_type::marker_durable:           value_ = token_type::marker_durable; break;
                case log_entry::entry_type::remove_entry:             value_ = token_type::remove_entry; break;
                case log_entry::entry_type::marker_invalidated_begin: value_ = token_type::marker_invalidated_begin; break;
                case log_entry::entry_type::clear_storage:            value_ = token_type::clear_storage; break;
                case log_entry::entry_type::add_storage:              value_ = token_type::add_storage; break;
                case log_entry::entry_type::remove_storage:           value_ = token_type::remove_storage; break;
                default: assert(false);
                }
            } else if (read_result.error().value() == log_entry::read_error::short_entry) {
                switch (read_result.error().entry_type()) {
                case log_entry::entry_type::normal_entry:             value_ = token_type::SHORT_normal_entry; break;
                case log_entry::entry_type::normal_with_blob:         value_ = token_type::SHORT_normal_with_blob; break;
                case log_entry::entry_type::marker_begin:             value_ = token_type::SHORT_marker_begin; break;
                case log_entry::entry_type::marker_end:               value_ = token_type::SHORT_marker_end; break;
                case log_entry::entry_type::marker_durable:           value_ = token_type::SHORT_marker_durable; break;
                case log_entry::entry_type::remove_entry:             value_ = token_type::SHORT_remove_entry; break;
                case log_entry::entry_type::marker_invalidated_begin: value_ = token_type::SHORT_marker_inv_begin; break;
                case log_entry::entry_type::clear_storage:            value_ = token_type::SHORT_clear_storage; break;
                case log_entry::entry_type::add_storage:              value_ = token_type::SHORT_add_storage; break;
                case log_entry::entry_type::remove_storage:           value_ = token_type::SHORT_remove_storage; break;
                default: assert(false);
                }
            } else if (read_result.error().value() == log_entry::read_error::unknown_type) {
                value_ = token_type::UNKNOWN_TYPE_entry;
            } else {
                assert(false);
            }
        }
        [[nodiscard]] token_type value() const noexcept { return value_; }

    private:
        token_type value_{0};
    };

// DFA
//
//  NOTE:
//    - This module currently fully accepts the old WAL format.
//    - In the old format, epoch snippets do not have explicit `marker_end` entries.
//      Each snippet ends implicitly when the next `marker_begin` or EOF appears.
//    - In the new format (future), each snippet *must* end with a `marker_end`.
//      If `marker_end` is missing in durable range, it will be treated as corruption.
//    - For now, this DFA does not enforce `marker_end` for durable epochs.
//      So `marker_begin` always implicitly closes any previous snippet.
//
//  START:
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_marker_begin         : { head_pos := ...; if (first) treat as leftover -> repair_by_cut or ignore; else { if (current_epoch <= ld) error-corrupted-durable else error-truncated } } -> END
//    SHORT_marker_inv_begin     : { already invalidated -> safe to ignore } -> END
//    marker_end                 : { error-unexpected } -> END
//    SHORT_marker_end           : { error-unexpected } -> END
//    UNKNOWN_TYPE_entry         : (not 1st) { if (valid && current_epoch <= ld) error-corrupted-durable else error-damaged-entry } -> END
//                               : (1st) { treat as leftover -> repair_by_mark or repair_by_cut } -> END
//    else                       : { err_unexpected } -> END
//
//  loop:
//    normal_entry               : { if (valid) process-entry } -> loop
//    normal_with_blob           : { if (valid) process-entry } -> loop
//    remove_entry               : { if (valid) process-entry } -> loop
//    clear_storage              : { if (valid) process-entry } -> loop
//    add_storage                : { if (valid) process-entry } -> loop
//    remove_storage             : { if (valid) process-entry } -> loop
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    marker_end                 : { mark end of snippet; reset state } -> loop
//    SHORT_normal_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_normal_with_blob     : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_clear_storage        : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_add_storage          : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_remove_storage       : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-truncated } -> END
//    SHORT_marker_begin         : { if (current_epoch <= ld) error-corrupted-durable else error-truncated } -> END
//    SHORT_marker_inv_begin     : { already invalidated -> safe to ignore } -> END
//    SHORT_marker_end           : { error-truncated } -> END
//    UNKNOWN_TYPE_entry         : { if (valid && current_epoch <= ld) error-corrupted-durable else if (valid) error-damaged-entry } -> END





// scan the file, and check max epoch number in this file
epoch_id_type dblog_scan::scan_one_pwal_file(  // NOLINT(readability-function-cognitive-complexity)
        const scan_input& input, epoch_id_type ld_epoch,
        const std::function<void(log_entry&)>& add_entry,
        const error_report_func_t& report_error,
        parse_error& pe) {
    const boost::filesystem::path& p = input.path();
    VLOG_LP(log_debug) << "processing pwal file: " << p.filename().string();
    epoch_id_type current_epoch{UINT64_MAX};
    epoch_id_type max_epoch_of_file{0};
    log_entry::read_error ec{};
    int fixed = 0;

    log_entry e;
    auto err_unexpected = [&](){
        log_entry::read_error ectmp{};
        ectmp.value(log_entry::read_error::unexpected_type);
        ectmp.entry_type(ec ? ec.entry_type() : e.type());
        report_error(ectmp);
    };
    // the file is opened read-only for reading. Rewriting the marker of an invalid epoch snippet opens it for writing
    // separately. The reader is held in an optional so that its lifetime can be ended explicitly to close the file
    // before trimming (resize_file)
    std::optional<log_entry_reader> reader{std::in_place, p};
    bool valid = true;  // scanning in the normal (not-invalidated) epoch snippet
    [[maybe_unused]]
    bool invalidated_wrote = true;  // invalid mark is wrote, so no need to mark again
    bool marked_before_scan{};  // scanning epoch-snippet already marked before this scan
    bool first = true;
    ec.value(log_entry::read_error::ok);
    std::streamoff fpos_epoch_snippet{};
    while (true) {
        if (input.stopped()) {
            break;  // the scan is aborted: leaves the rest unread and returns after the cleanup only
        }
        const auto read_result = reader->next();
        const auto fpos_before_read_entry = static_cast<std::streamoff>(read_result.offset());
        if (read_result.result() == log_entry_read_result::status::ok) {
            e = log_entry{read_result.view()};
            ec = log_entry::read_error{};
        } else {
            e = log_entry{};
            ec = read_result.result() == log_entry_read_result::status::error ? read_result.error()
                                                                              : log_entry::read_error{};
        }
        VLOG_LP(45) << "read: { result:" << read_result.result() << ", ec:" << ec.value() << " : " << ec.message()
                    << ", e:" << static_cast<int>(e.type()) << "}";
        lex_token tok{read_result};
        VLOG_LP(45) << "token: " << static_cast<int>(tok.value());
        bool aborted = false;
        switch (tok.value()) {
        case lex_token::token_type::normal_entry:
        case lex_token::token_type::normal_with_blob:
        case lex_token::token_type::remove_entry:
        case lex_token::token_type::clear_storage:
        case lex_token::token_type::add_storage:
        case lex_token::token_type::remove_storage:
// normal_entry | normal_with_blob | remove_entry | clear_storage | add_storage | remove_storage : (not 1st) { if (valid) process-entry } -> loop
            if (!first) {
                if (valid) {
                    add_entry(e);
                }
            } else {
                err_unexpected();
                pe = parse_error(parse_error::unexpected, fpos_before_read_entry);
                if (fail_fast_) aborted = true;
            }
            break;
        case lex_token::token_type::eof:
            aborted = true;
            break;
        case lex_token::token_type::marker_begin: {
// marker_begin : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
            fpos_epoch_snippet = fpos_before_read_entry;
            current_epoch = e.epoch_id();
            max_epoch_of_file = std::max(max_epoch_of_file, current_epoch);
            marked_before_scan = false;
            if (current_epoch <= ld_epoch) {
                valid = true;
                invalidated_wrote = false;
                VLOG_LP(45) << "valid: true";
            } else {
                // exists-epoch-snippet-after-durable-epoch
                switch (process_at_nondurable_) {
                case process_at_nondurable::ignore:
                    invalidated_wrote = false;
                    break;
                case process_at_nondurable::repair_by_mark:
                    invalidate_epoch_snippet(p, fpos_epoch_snippet);
                    VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                    fixed++;
                    invalidated_wrote = true;
                    if (pe.value() < parse_error::repaired) {
                        pe = parse_error(parse_error::repaired);
                    }
                    break;
                //  case process_at_nondurable::repair_by_cut:
                //      THROW_LIMESTONE_EXCEPTION("unimplemented repair method");
                case process_at_nondurable::report:
                    invalidated_wrote = false;
                    log_entry::read_error nondurable(log_entry::read_error::nondurable_snippet);
                    report_error(nondurable);
                    if (pe.value() < parse_error::nondurable_entries) {
                        pe = parse_error(parse_error::nondurable_entries);
                    }
                }
                valid = false;
                VLOG_LP(45) << "valid: false";
            }
            first = false;
            break;
        }
        case lex_token::token_type::marker_invalidated_begin: {
// marker_invalidated_begin : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
            fpos_epoch_snippet = fpos_before_read_entry;
            max_epoch_of_file = std::max(max_epoch_of_file, e.epoch_id());
            marked_before_scan = true;
            invalidated_wrote = true;
            valid = false;
            VLOG_LP(45) << "valid: false (already marked)";
            first = false;
            break;
        }
        case lex_token::token_type::SHORT_normal_entry:
        case lex_token::token_type::SHORT_normal_with_blob:
        case lex_token::token_type::SHORT_remove_entry:
        case lex_token::token_type::SHORT_clear_storage:
        case lex_token::token_type::SHORT_add_storage:
        case lex_token::token_type::SHORT_remove_storage:
        case lex_token::token_type::SHORT_marker_end: {
// SHORT_* : (not 1st) { if (valid && durable) corrupted else if (valid) truncated } -> END
            if (first) {
                err_unexpected();
                pe = parse_error(parse_error::unexpected, fpos_before_read_entry);
            } else if (valid && current_epoch <= ld_epoch) {
                report_error(ec);
                pe = parse_error(parse_error::corrupted_durable_entries, fpos_epoch_snippet);
            } else {
                switch (process_at_truncated_) {
                case process_at_truncated::ignore:
                    break;
                case process_at_truncated::repair_by_mark:
                    if (valid) {
                        invalidate_epoch_snippet(p, fpos_epoch_snippet);
                        fixed++;
                        VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                        // quitting loop just after this, so no need to change 'valid', but...
                        valid = false;
                        VLOG_LP(45) << "valid: false";
                    }
                    if (pe.value() < parse_error::broken_after_marked) {
                        pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    }
                    break;
                case process_at_truncated::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;
                case process_at_truncated::report:
                    if (valid) {
                        // durable broken data, serious
                        report_error(ec);
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    } else if (marked_before_scan) {
                        if (pe.value() < parse_error::broken_after_marked) {
                            pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                        }
                    } else {
                        // marked during inspect
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    }
                }
            }
            aborted = true;
            break;
        }
        case lex_token::token_type::SHORT_marker_begin: {
// SHORT_marker_begin : { head_pos := ...; error-truncated } -> END
            fpos_epoch_snippet = fpos_before_read_entry;
            marked_before_scan = false;
            if (!first && current_epoch <= ld_epoch) {
                report_error(ec);
                pe = parse_error(parse_error::corrupted_durable_entries, fpos_epoch_snippet);
            } else {
                switch (process_at_truncated_) {
                    case process_at_truncated::ignore:
                        break;
                    case process_at_truncated::repair_by_mark:
                        invalidate_epoch_snippet(p, fpos_epoch_snippet);
                        fixed++;
                        VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                        pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                        break;
                    case process_at_truncated::repair_by_cut:
                        pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                        break;
                    case process_at_truncated::report:
                        report_error(ec);
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                }
            }
            aborted = true;
            break;
        }
        case lex_token::token_type::SHORT_marker_inv_begin: {
        // SHORT_marker_inv_begin : partial invalidated marker -> safe to ignore
            fpos_epoch_snippet = fpos_before_read_entry;
            marked_before_scan = true;
            invalidated_wrote = true;
            valid = false;
            VLOG_LP(45) << "valid: false (already marked)";
            first = false;
            break;
        }
        case lex_token::token_type::UNKNOWN_TYPE_entry: {
        // UNKNOWN_TYPE_entry : (not 1st) { if (valid && current_epoch <= ld) error-corrupted-durable else error-damaged-entry } -> END
        // UNKNOWN_TYPE_entry : (1st) { treat as leftover -> repair_by_mark or repair_by_cut } -> END
            if (first) {
                fpos_epoch_snippet = fpos_before_read_entry;

                switch (process_at_damaged_) {
                case process_at_damaged::ignore:
                    break;

                case process_at_damaged::repair_by_mark:
                    invalidate_epoch_snippet(p, fpos_epoch_snippet);
                    fixed++;
                    VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                    if (pe.value() < parse_error::broken_after_marked) {
                        pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    }
                    break;

                case process_at_damaged::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;

                case process_at_damaged::report:
                    err_unexpected();
                    report_error(ec);
                    pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    break;
                }
            } else if (valid && current_epoch <= ld_epoch) {
                report_error(ec);
                pe = parse_error(parse_error::corrupted_durable_entries, fpos_epoch_snippet);
            } else {
                switch (process_at_damaged_) {
                case process_at_damaged::ignore:
                    break;
                case process_at_damaged::repair_by_mark:
                    if (valid) {
                        invalidate_epoch_snippet(p, fpos_epoch_snippet);
                        fixed++;
                        VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                        // quitting loop just after this, so no need to change 'valid', but...
                        valid = false;
                        VLOG_LP(45) << "valid: false";
                    }
                    if (pe.value() < parse_error::broken_after_marked) {
                        pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    }
                    break;
                case process_at_damaged::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;
                case process_at_damaged::report:
                    if (valid) {
                        // durable broken data, serious
                        report_error(ec);
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    } else if (marked_before_scan) {
                        if (pe.value() < parse_error::broken_after_marked) {
                            pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                        }
                    } else {
                        // marked during inspect
                        pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                    }
                }
            }
            aborted = true;
            break;
        }
        case lex_token::token_type::marker_end: {
            if (first) {
                err_unexpected();
                pe = parse_error(parse_error::unexpected, fpos_before_read_entry);
                aborted = true;
            } else {
                VLOG_LP(45) << "marker_end: closing current snippet";
                valid = false;
                first = true;
            }
            break;
        }
        default:
            // unexpected log_entry; may be logical error of program, not by disk damage
            err_unexpected();
            if (tok.value() >= lex_token::token_type::SHORT_normal_entry || fail_fast_) {
                aborted = true;
            }
            pe = parse_error(parse_error::unexpected, fpos_before_read_entry);  // point to this log_entry
        }
        if (aborted) break;
    }
    reader.reset();  // close the file before trimming
    if (pe.value() == parse_error::broken_after_tobe_cut) {
        // DO trim
        // TODO: check byte at fpos is 0x02 or 0x06
        // throw limestone_io_exception if the file cannot be trimmed: the exception of boost would not be caught by
        // the worker threads of the scan
        boost::system::error_code error;
        boost::filesystem::resize_file(p, pe.fpos(), error);
        if (error) {
            LOG_AND_THROW_IO_EXCEPTION("cannot trim pwal file: " + p.string(), error);
        }
        VLOG_LP(0) << "trimmed " << p << " at offset " << pe.fpos();
        pe.value(parse_error::repaired);
        fixed++;
    }
    VLOG_LP(log_debug) << "fixed: " << fixed;
    pe.modified(fixed > 0);
    return max_epoch_of_file;
}

}
