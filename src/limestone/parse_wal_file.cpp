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

#include <boost/filesystem/fstream.hpp>
#include <cstdlib>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "dblog_scan.h"
#include "log_entry.h"

namespace limestone::internal {
using namespace limestone::api;

void invalidate_epoch_snippet(boost::filesystem::fstream& strm, std::streampos fpos_head_of_epoch_snippet) {
    auto pos = strm.tellg();
    strm.seekp(fpos_head_of_epoch_snippet, std::ios::beg);
    char buf = static_cast<char>(log_entry::entry_type::marker_invalidated_begin);
    strm.write(&buf, sizeof(char));
    strm.flush();
    // TODO fsync
    strm.seekg(pos, std::ios::beg);  // restore position
    if (!strm) {
        LOG_LP(ERROR) << "I/O error at marking epoch snippet header";
    }
}

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
//                                 | remove_entry
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
//                                 | remove_entry             { if (valid) process-entry }
//                                 | SHORT_normal_entry       { if (valid) error-truncated }  // TAIL
//                                 | SHORT_remove_entry       { if (valid) error-truncated }  // TAIL
//                                 | UNKNOWN_TYPE_entry       { if (valid) error-damaged-entry }  // TAIL
//   snippet_footer                = (empty)

// lexer rule (see log_entry.h)
//   marker_begin                  = 0x02 epoch
//   marker_invalidated_begin      = 0x06 epoch
//   normal_entry                  = 0x01 key_length value_length storage_id key(key_length) write_version_major write_version_minor value(value_length)
//   remove_entry                  = 0x05 key_length storage_id key(key_length) writer_version_major writer_version_minor
//   marker_durable                = 0x04 epoch
//   marker_end                    = 0x03 epoch
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
//   SHORT_remove_entry            = 0x05 key_length storage_id key(key_length) byte(0-15)
//                                 = 0x05 key_length storage_id key(<key_length)
//                                 = 0x05 byte(0-11)
//   SHORT_marker_durable          = 0x04 byte(0-7)
//   SHORT_marker_end              = 0x03 byte(0-7)
//   UNKNOWN_TYPE_entry            = 0x00 byte(0-)
//                                 | 0x07-0xff byte(0-)
//   // marker_durable and marker_end are not used in pWAL file
//   // SHORT_*, UNKNOWN_* appears just before EOF
    class lex_token {
    public:
        enum class token_type {
            eof,
            normal_entry = 1, marker_begin, marker_end, marker_durable, remove_entry, marker_invalidated_begin,
            SHORT_normal_entry = 101, SHORT_marker_begin, SHORT_marker_end, SHORT_marker_durable, SHORT_remove_entry, SHORT_marker_inv_begin,
            UNKNOWN_TYPE_entry = 1001,
        };

        lex_token(log_entry::read_error& ec, bool data_remains, log_entry& e) {
            set(ec, data_remains, e);
        }
        void set(log_entry::read_error& ec, bool data_remains, log_entry& e) {
            if (ec.value() == 0) {
                if (!data_remains) {
                    value_ = token_type::eof;
                } else switch (e.type()) {  // NOLINT(*braces-around-statements)
                case log_entry::entry_type::normal_entry:             value_ = token_type::normal_entry; break;
                case log_entry::entry_type::marker_begin:             value_ = token_type::marker_begin; break;
                case log_entry::entry_type::marker_end:               value_ = token_type::marker_end; break;
                case log_entry::entry_type::marker_durable:           value_ = token_type::marker_durable; break;
                case log_entry::entry_type::remove_entry:             value_ = token_type::remove_entry; break;
                case log_entry::entry_type::marker_invalidated_begin: value_ = token_type::marker_invalidated_begin; break;
                default: assert(false);
                }
            } else if (ec.value() == log_entry::read_error::short_entry) {
                switch (e.type()) {
                case log_entry::entry_type::normal_entry:             value_ = token_type::SHORT_normal_entry; break;
                case log_entry::entry_type::marker_begin:             value_ = token_type::SHORT_marker_begin; break;
                case log_entry::entry_type::marker_end:               value_ = token_type::SHORT_marker_end; break;
                case log_entry::entry_type::marker_durable:           value_ = token_type::SHORT_marker_durable; break;
                case log_entry::entry_type::remove_entry:             value_ = token_type::SHORT_remove_entry; break;
                case log_entry::entry_type::marker_invalidated_begin: value_ = token_type::SHORT_marker_inv_begin; break;
                default: assert(false);
                }
            } else if (ec.value() == log_entry::read_error::unknown_type) {
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
//  START:
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_marker_begin         : { head_pos := ...; error-truncated } -> END
//    SHORT_marker_inv_begin     : { head_pos := ... } -> END
//    UNKNOWN_TYPE_entry         : { error-broken-snippet-header } -> END
//    else                       : { err_unexpected } -> END
//  loop:
//    normal_entry               : { if (valid) process-entry } -> loop
//    remove_entry               : { if (valid) process-entry } -> loop
//    eof                        : {} -> END
//    marker_begin               : { head_pos := ...; max-epoch := max(...); if (epoch <= ld) { valid := true } else { valid := false, error-nondurable } } -> loop
//    marker_invalidated_begin   : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
//    SHORT_normal_entry         : { if (valid) error-truncated } -> END
//    SHORT_remove_entry         : { if (valid) error-truncated } -> END
//    SHORT_marker_begin         : { head_pos := ...; error-truncated } -> END
//    SHORT_marker_inv_begin     : { head_pos := ... } -> END
//    UNKNOWN_TYPE_entry         : { if (valid) error-damaged-entry } -> END


// scan the file, and check max epoch number in this file
epoch_id_type dblog_scan::scan_one_pwal_file(  // NOLINT(readability-function-cognitive-complexity)
        const boost::filesystem::path& p, epoch_id_type ld_epoch,
        const std::function<void(log_entry&)>& add_entry,
        const error_report_func_t& report_error,
        parse_error& pe) {
    VLOG_LP(log_info) << "processing pwal file: " << p.filename().string();
    epoch_id_type current_epoch{UINT64_MAX};
    epoch_id_type max_epoch_of_file{0};
    log_entry::read_error ec{};

    log_entry e;
    auto err_unexpected = [&](){
        log_entry::read_error ectmp{};
        ectmp.value(log_entry::read_error::unexpected_type);
        ectmp.entry_type(e.type());
        report_error(ectmp);
    };
    boost::filesystem::fstream strm;
    strm.open(p, std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    if (!strm) {
        LOG_LP(ERROR) << "cannot read pwal file: " << p;
        throw std::runtime_error("cannot read pwal file");
    }
    bool valid = true;  // scanning in the normal (not-invalidated) epoch snippet
    [[maybe_unused]]
    bool invalidated_wrote = true;  // invalid mark is wrote, so no need to mark again
    bool first = true;
    ec.value(log_entry::read_error::ok);
    std::streampos fpos_epoch_snippet;
    while (true) {
        auto fpos_before_read_entry = strm.tellg();
        bool data_remains = e.read_entry_from(strm, ec);
        VLOG_LP(45) << "read: { ec:" << ec.value() << " : " << ec.message() << ", data_remains:" << data_remains << ", e:" << static_cast<int>(e.type()) << "}";
        lex_token tok{ec, data_remains, e};
        VLOG_LP(45) << "token: " << static_cast<int>(tok.value());
        bool aborted = false;
        switch (tok.value()) {
        case lex_token::token_type::normal_entry:
        case lex_token::token_type::remove_entry:
// normal_entry | remove_entry : (not 1st) { if (valid) process-entry } -> loop
            if (!first) {
                if (valid) {
                    add_entry(e);
                }
            } else {
                err_unexpected();
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
                    invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                    VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                    invalidated_wrote = true;
                    if (pe.value() < parse_error::repaired) {
                        pe = parse_error(parse_error::repaired);
                    }
                    break;
                // case process_at_nondurable::repair_by_cut:
                //     throw std::runtime_error("unimplemented repair method");
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
            break;
        }
        case lex_token::token_type::marker_invalidated_begin: {
// marker_invalidated_begin : { head_pos := ...; max-epoch := max(...); valid := false } -> loop
            fpos_epoch_snippet = fpos_before_read_entry;
            max_epoch_of_file = std::max(max_epoch_of_file, e.epoch_id());
            invalidated_wrote = true;
            valid = false;
            VLOG_LP(45) << "valid: false (already marked)";
            break;
        }
        case lex_token::token_type::SHORT_normal_entry:
        case lex_token::token_type::SHORT_remove_entry: {
// SHORT_normal_entry | SHORT_remove_entry : (not 1st) { if (valid) error-truncated } -> END
            if (first) {
                err_unexpected();
            } else {
                switch (process_at_truncated_) {
                case process_at_truncated::ignore:
                    break;
                case process_at_truncated::repair_by_mark:
                    strm.clear();  // reset eof
                    invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                    VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                    pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    break;
                case process_at_truncated::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;
                case process_at_truncated::report:
                    if (valid) {
                        // durable broken data, serious
                        report_error(ec);
                    }
                    pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                }
            }
            aborted = true;
            break;
        }
        case lex_token::token_type::SHORT_marker_begin: {
// SHORT_marker_begin : { head_pos := ...; error-truncated } -> END
            fpos_epoch_snippet = fpos_before_read_entry;
            switch (process_at_truncated_) {
            case process_at_truncated::ignore:
                break;
            case process_at_truncated::repair_by_mark:
                strm.clear();  // reset eof
                invalidate_epoch_snippet(strm, fpos_epoch_snippet);
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
            aborted = true;
            break;
        }
        case lex_token::token_type::SHORT_marker_inv_begin: {
// SHORT_marker_inv_begin : { head_pos := ... } -> END
            fpos_epoch_snippet = fpos_before_read_entry;
            // ignore short in invalidated blocks
            switch (process_at_truncated_) {
            case process_at_truncated::ignore:
                break;
            case process_at_truncated::repair_by_mark:
                strm.clear();  // reset eof
                invalidate_epoch_snippet(strm, fpos_epoch_snippet);
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
            aborted = true;
            break;
        }
        case lex_token::token_type::UNKNOWN_TYPE_entry: {
// UNKNOWN_TYPE_entry : (not 1st) { if (valid) error-damaged-entry } -> END
// UNKNOWN_TYPE_entry : (1st) { error-broken-snippet-header } -> END
            if (first) {
                err_unexpected();  // FIXME: error type
            } else {
                switch (process_at_damaged_) {
                case process_at_damaged::ignore:
                    break;
                case process_at_damaged::repair_by_mark:
                    strm.clear();  // reset eof
                    invalidate_epoch_snippet(strm, fpos_epoch_snippet);
                    VLOG_LP(0) << "marked invalid " << p << " at offset " << fpos_epoch_snippet;
                    pe = parse_error(parse_error::broken_after_marked, fpos_epoch_snippet);
                    break;
                case process_at_damaged::repair_by_cut:
                    pe = parse_error(parse_error::broken_after_tobe_cut, fpos_epoch_snippet);
                    break;
                case process_at_damaged::report:
                    if (valid) {
                        // durable broken data, serious
                        report_error(ec);
                    }
                    pe = parse_error(parse_error::broken_after, fpos_epoch_snippet);
                }
            }
            aborted = true;
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
        first = false;
    }
    strm.close();
    if (pe.value() == parse_error::broken_after_tobe_cut) {
        // DO trim
        // TODO: check byte at fpos is 0x02 or 0x06
        boost::filesystem::resize_file(p, pe.fpos());
        VLOG_LP(0) << "trimmed " << p << " at offset " << pe.fpos();
        pe.value(parse_error::repaired);
    }
    return max_epoch_of_file;
}

}
