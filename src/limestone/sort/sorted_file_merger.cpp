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
#include "sort/sorted_file_merger.h"

#include <memory>
#include <queue>
#include <sstream>
#include <utility>

#include <boost/system/error_code.hpp>
#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "log_entry/log_entry_format.h"
#include "logging_helper.h"
#include "log_entry/log_entry_reader.h"
#include "sort/key_sid_deduplicator.h"
#include "sort/sorted_file_writer.h"

namespace limestone::internal {

namespace format = log_entry_format;

namespace {

/// one input of the merge: the file, its reader and its current entry
struct merge_source {
    boost::filesystem::path file;
    std::unique_ptr<log_entry_reader> reader;
    /// the current entry. In the default-constructed state (type this_id_is_not_used) at the end of the file
    log_entry_view current;
    /// called each time a normal_with_blob is read. Not called if empty
    sorted_file_merger::entry_callback on_normal_with_blob;
    /// whether nothing has been read yet (the first read skips a marker_begin at the beginning)
    bool unread{true};
};

/**
 * @brief Reads the next data entry from an input.
 *
 * If the first entry of the file is a marker_begin (a compacted WAL file), it is skipped.
 * @param source the input
 * @return the next entry, or nullopt at the end of the file
 * @throws limestone_io_exception if reading fails (with errno), or if the contents of the file do not match the
 *         format (without errno): the file ends in the middle of an entry, the entry type is invalid, or an entry is
 *         not a data entry
 */
std::optional<log_entry_view> read_next_entry(merge_source& source) {
    log_entry_read_result r = source.reader->next();
    if (source.unread) {
        source.unread = false;
        if (r.result() == log_entry_read_result::status::ok && r.view().type() == format::entry_type::marker_begin) {
            r = source.reader->next();
        }
    }
    switch (r.result()) {
    case log_entry_read_result::status::ok:
        if (! format::is_data_entry(r.view().type())) {
            std::ostringstream message;
            message << "merge input is broken (non-data entry " << r.view().type() << ") at offset " << r.offset()
                    << ": " << source.file.string();
            LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO(message.str());
        }
        return r.view();
    case log_entry_read_result::status::end:
        return std::nullopt;
    case log_entry_read_result::status::error: {
        std::ostringstream message;
        message << "merge input is broken (" << r.error().message() << ") at offset " << r.offset() << ": "
                << source.file.string();
        LOG_AND_THROW_IO_EXCEPTION_NO_ERRNO(message.str());
    }
    }
    LOG_LP(FATAL) << "unexpected read result " << r.result();
    return std::nullopt;
}

/**
 * @brief The order of the current entries of the inputs. As the comparator of std::priority_queue, it takes the
 *        numbers of two inputs and compares their current entries.
 *
 * operator() returns true if the current entry of input a is greater than that of input b (later in the order of
 * the output: a greater key_sid, or the same key_sid and a smaller write_version). It has the same direction as
 * std::greater, so the top of the std::priority_queue is the least.
 */
class current_entry_order {
public:
    explicit current_entry_order(const std::vector<merge_source>& sources) noexcept : sources_(&sources) {}
    bool operator()(std::size_t a, std::size_t b) const {
        const log_entry_view& va = (*sources_)[a].current;
        const log_entry_view& vb = (*sources_)[b].current;
        const int c = format::compare_key_sid(va.serialized_key_sid(), vb.serialized_key_sid());
        if (c != 0) {
            return c > 0;
        }
        return va.write_version() < vb.write_version();
    }
private:
    const std::vector<merge_source>* sources_;
};

/**
 * @brief A k-way merge. Takes the entries out of multiple inputs in ascending order of key_sid (and in descending
 *        order of write_version within the same key_sid).
 *
 * The heap holds the numbers of the inputs, ordered by their current entries. Changing the current entry of an
 * input while it is in the heap breaks the order, so an input is advanced only after it is taken out of the heap
 * (at the beginning of the next next()).
 */
class k_way_merge {
public:
    /**
     * @brief Reads the first entry of each input.
     * @param sources the inputs
     * @throws limestone_io_exception if reading fails
     */
    explicit k_way_merge(std::vector<merge_source> sources)
        : sources_(std::move(sources)), heap_(current_entry_order(sources_)) {
        for (std::size_t i = 0; i < sources_.size(); ++i) {
            advance(i);
        }
    }

    /**
     * @brief Returns the next entry (the least among all the inputs).
     *
     * The returned view is valid until next() is called next.
     * @return the next entry, or nullopt if all the inputs are at their ends
     * @throws limestone_io_exception if reading fails
     */
    std::optional<log_entry_view> next() {
        if (returned_) {
            // advances the input of the entry returned last; the returned view becomes invalid here
            advance(*returned_);
            returned_.reset();
        }
        if (heap_.empty()) {
            return std::nullopt;
        }
        const std::size_t i = heap_.top();
        heap_.pop();
        returned_ = i;
        return sources_[i].current;
    }

private:
    /**
     * @brief Reads the next entry of input i, and puts the input into the heap if there is one.
     * @param i the number of the input
     * @throws limestone_io_exception if reading fails
     */
    void advance(std::size_t i) {
        merge_source& source = sources_[i];
        const std::optional<log_entry_view> entry = read_next_entry(source);
        if (! entry) {
            source.current = log_entry_view{};
            return;
        }
        source.current = *entry;
        if (source.on_normal_with_blob && source.current.type() == format::entry_type::normal_with_blob) {
            source.on_normal_with_blob(source.current);
        }
        heap_.push(i);
    }

    std::vector<merge_source> sources_;
    /// the heap for the merge. It holds the numbers of the inputs that have a current entry, ordered by
    /// current_entry_order so that the top is the input whose current entry is the least (std::priority_queue used
    /// as a min-heap). The template arguments are the element type (the number of an input = the index into
    /// sources_), the container for the array of the heap (vector, the same as the default; written to give the
    /// third argument), and the comparator.
    std::priority_queue<std::size_t, std::vector<std::size_t>, current_entry_order> heap_;
    /// the number of the input of the entry returned by the last next()
    std::optional<std::size_t> returned_{};
};

/**
 * @brief Opens one input.
 * @param file the file of the input
 * @param on_normal_with_blob the callback called each time a normal_with_blob is read. Not called if empty
 * @return the input. Its current entry is not read yet (empty)
 * @throws limestone_io_exception if the file cannot be opened
 */
merge_source open_source(const boost::filesystem::path& file,
                         const sorted_file_merger::entry_callback& on_normal_with_blob) {
    merge_source source;
    source.file = file;
    source.reader = std::make_unique<log_entry_reader>(file);
    source.on_normal_with_blob = on_normal_with_blob;
    return source;
}

/**
 * @brief Opens the inputs of a merge.
 * @param sorted_files the sorted files
 * @param compacted the compacted WAL file added last, or nullopt
 * @return the inputs, in the order of sorted_files, followed by compacted if any
 * @throws limestone_io_exception if a file cannot be opened
 */
std::vector<merge_source> open_sources(const std::vector<boost::filesystem::path>& sorted_files,
                                       const std::optional<sorted_file_merger::compacted_wal_input>& compacted) {
    std::vector<merge_source> sources;
    sources.reserve(sorted_files.size() + 1);
    for (const auto& file : sorted_files) {
        sources.push_back(open_source(file, {}));
    }
    if (compacted) {
        sources.push_back(open_source(compacted->file, compacted->on_normal_with_blob));
    }
    return sources;
}

/**
 * @brief Removes a sorted file.
 * @param file the file to remove
 * @throws limestone_io_exception if the removal fails (with errno)
 */
void remove_sorted_file(const boost::filesystem::path& file) {
    boost::system::error_code ec;
    boost::filesystem::remove(file, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("cannot remove sorted file: " + file.string(), ec.value());
    }
}

}  // namespace

sorted_file_merger::sorted_file_merger(sorting_directory& directory, std::size_t merge_fanin)
    : directory_(&directory), merge_fanin_(merge_fanin) {
    if (merge_fanin < 2) {
        LOG_LP(FATAL) << "merge fanin " << merge_fanin << " must be at least 2";
    }
}

void sorted_file_merger::merge(std::vector<boost::filesystem::path> sorted_files,
                               const std::optional<compacted_wal_input>& compacted, const range_deletions& deletions,
                               const entry_callback& output) {
    if (! output) {
        LOG_LP(FATAL) << "output must not be empty";
    }
    // intermediate merges: combine fan-in files at a time until the number of inputs (including the compacted WAL
    // file) is within the fan-in
    const std::size_t extra = compacted ? 1 : 0;
    while (sorted_files.size() + extra > merge_fanin_) {
        const auto batch_end = sorted_files.begin() + static_cast<std::ptrdiff_t>(merge_fanin_);
        const std::vector<boost::filesystem::path> batch(sorted_files.begin(), batch_end);
        const boost::filesystem::path merged = intermediate_merge(batch);
        sorted_files.erase(sorted_files.begin(), batch_end);
        sorted_files.push_back(merged);
    }

    // the final merge: remove duplicates, apply the range deletions, and pass the entries to output
    k_way_merge merged(open_sources(sorted_files, compacted));
    key_sid_deduplicator deduplicator;
    while (const auto entry = merged.next()) {
        if (! deduplicator.accept(*entry)) {
            continue;
        }
        if (! deletions.empty()) {
            const auto deletion = deletions.find(entry->storage());
            if (deletion != deletions.end() && entry->write_version() < deletion->second) {
                continue;
            }
        }
        output(*entry);
    }
}

boost::filesystem::path sorted_file_merger::intermediate_merge(const std::vector<boost::filesystem::path>& inputs) {
    const boost::filesystem::path merged_file = directory_->new_file_path();
    {
        k_way_merge merged(open_sources(inputs, std::nullopt));
        sorted_file_writer writer(merged_file);
        while (const auto entry = merged.next()) {
            writer.write(*entry);  // the writer removes duplicates
        }
        writer.close();
    }  // the inputs are closed before being removed
    for (const auto& input : inputs) {
        remove_sorted_file(input);
    }
    return merged_file;
}

}  // namespace limestone::internal
