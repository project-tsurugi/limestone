/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include "compaction_carry_writer.h"

#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <utility>

#include <glog/logging.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

compaction_carry_writer::compaction_carry_writer(epoch_id_type boundary_epoch, boost::filesystem::path out_path)
    : boundary_epoch_(boundary_epoch), out_path_(std::move(out_path)) {
}

// Enumerates the byte ranges of the snippets beyond the boundary (beginning with
// marker_begin, epoch > boundary). A snippet ends at the next snippet header
// (marker_begin / marker_invalidated_begin) or at EOF, so the marker_end in between
// is part of the snippet and is copied with it.
std::vector<compaction_carry_writer::snippet_span>
compaction_carry_writer::collect_carry_spans(const boost::filesystem::path& input) const {
    std::vector<snippet_span> spans;
    std::ifstream in(input.string(), std::ios::in | std::ios::binary);
    if (!in) {
        LOG_AND_THROW_IO_EXCEPTION("cannot open the input file of the carry pass: " + input.string(), errno);
    }
    log_entry e;
    bool carrying = false;
    std::streamoff carry_start = 0;
    for (;;) {
        std::streamoff pos = in.tellg();
        if (!e.read(in)) {  // a broken entry makes log_entry::read throw
            if (carrying) {
                spans.push_back({carry_start, pos - carry_start});
            }
            break;
        }
        if (e.type() == log_entry::entry_type::marker_begin ||
            e.type() == log_entry::entry_type::marker_invalidated_begin) {
            if (carrying) {
                spans.push_back({carry_start, pos - carry_start});
                carrying = false;
            }
            if (e.type() == log_entry::entry_type::marker_begin && e.epoch_id() > boundary_epoch_) {
                carrying = true;
                carry_start = pos;
            }
        }
    }
    return spans;
}

// Copies each byte range of the input to out, unmodified.
void compaction_carry_writer::copy_spans(const boost::filesystem::path& input, const std::vector<snippet_span>& spans, FILE* out) {
    std::ifstream in(input.string(), std::ios::in | std::ios::binary);
    if (!in) {
        LOG_AND_THROW_IO_EXCEPTION("cannot open the input file of the carry pass: " + input.string(), errno);
    }
    std::vector<char> buffer(64UL * 1024UL);
    for (const snippet_span& span : spans) {
        if (!in.seekg(span.offset)) {
            LOG_AND_THROW_IO_EXCEPTION("failed to seek the input file of the carry pass: " + input.string(), errno);
        }
        std::streamoff remaining = span.length;
        while (remaining > 0) {
            auto chunk = static_cast<std::streamsize>(std::min<std::streamoff>(remaining, static_cast<std::streamoff>(buffer.size())));
            in.read(buffer.data(), chunk);
            if (in.gcount() != chunk) {
                if (in.bad()) {
                    LOG_AND_THROW_IO_EXCEPTION("failed to read the input file of the carry pass: " + input.string(), errno);
                }
                LOG_AND_THROW_EXCEPTION("failed to read the input file of the carry pass (unexpected end): " + input.string());
            }
            if (fwrite(buffer.data(), 1, static_cast<std::size_t>(chunk), out) != static_cast<std::size_t>(chunk)) {
                LOG_AND_THROW_IO_EXCEPTION("failed to write the carry file", errno);
            }
            remaining -= chunk;
        }
    }
}

bool compaction_carry_writer::write(const std::vector<boost::filesystem::path>& inputs) {
    FILE* out = nullptr;
    try {
        for (const boost::filesystem::path& input : inputs) {
            std::vector<snippet_span> spans = collect_carry_spans(input);
            if (spans.empty()) {
                continue;
            }
            if (out == nullptr) {
                // Create the output only when a snippet to carry is found, so that no
                // empty carry file is left behind.
                out = fopen(out_path_.c_str(), "wb");  // NOLINT(*-owning-memory)
                if (out == nullptr) {
                    LOG_AND_THROW_IO_EXCEPTION("cannot create the carry file: " + out_path_.string(), errno);
                }
            }
            copy_spans(input, spans, out);
        }
    } catch (...) {
        if (out != nullptr) {
            (void) fclose(out);  // NOLINT(*-owning-memory) cleanup while propagating; failure ignored
        }
        throw;
    }
    if (out == nullptr) {
        return false;
    }
    if (fflush(out) != 0) {
        int saved_errno = errno;
        (void) fclose(out);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot flush the carry file: " + out_path_.string(), saved_errno);
    }
    if (fsync(fileno(out)) != 0) {
        int saved_errno = errno;
        (void) fclose(out);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot fsync the carry file: " + out_path_.string(), saved_errno);
    }
    if (fclose(out) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close the carry file: " + out_path_.string(), errno);
    }
    return true;
}

}  // namespace limestone::internal
