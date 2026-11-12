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

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include "file_operations.h"
#include "log_entry/log_entry_reader.h"
#include "log_entry_bytes.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::api::log_entry;
using limestone::internal::file_write_state;
using limestone::internal::file_write_state_function;
using limestone::internal::log_entry_read_result;
using limestone::internal::log_entry_reader;
using limestone::internal::log_entry_view;
using limestone::internal::real_file_operations;
using status = limestone::internal::log_entry_read_result::status;
using entry_type = limestone::internal::log_entry_format::entry_type;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::u32le;
using log_entry_bytes::u8;

constexpr std::size_t buffer_size = log_entry_reader::default_buffer_size;

// file operations that record the sizes requested by pread and can limit the amount returned by one call
class recording_file_operations : public real_file_operations {
public:
    explicit recording_file_operations(std::size_t max_read = SIZE_MAX) : max_read_(max_read) {}

    ssize_t pread(int fd, void* buf, size_t count, off_t offset) override {
        requests_.push_back(count);
        return real_file_operations::pread(fd, buf, std::min(count, max_read_), offset);
    }

    [[nodiscard]] const std::vector<std::size_t>& requests() const noexcept { return requests_; }

private:
    std::size_t max_read_;
    std::vector<std::size_t> requests_{};
};

class log_entry_reader_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/log_entry_reader_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    // creates a file containing the bytes, and returns its path
    static boost::filesystem::path write_file(std::string_view bytes) {
        boost::filesystem::path p = boost::filesystem::path(location) / "data";
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }

    // checks that the result is the entry `entry` (at position `offset`)
    static void expect_entry(log_entry_reader& reader, std::string_view entry, std::uint64_t offset) {
        auto r = reader.next();
        ASSERT_EQ(r.result(), status::ok);
        EXPECT_EQ(r.offset(), offset);
        EXPECT_TRUE(r.view().serialized_entry() == entry) << "size " << r.view().serialized_entry().size()
                                                          << " expected " << entry.size();
    }

    // checks that the result is the end (at position `offset`)
    static void expect_end(log_entry_reader& reader, std::uint64_t offset) {
        auto r = reader.next();
        ASSERT_EQ(r.result(), status::end);
        EXPECT_EQ(r.offset(), offset);
    }

    // checks that the result is a failure (kind `code`, entry type `type`, at position `offset`)
    static void expect_error(log_entry_reader& reader, log_entry::read_error::code code, entry_type type,
                             std::uint64_t offset) {
        auto r = reader.next();
        ASSERT_EQ(r.result(), status::error);
        EXPECT_EQ(r.offset(), offset);
        EXPECT_EQ(r.error().value(), code);
        EXPECT_EQ(r.error().entry_type(), type);
    }
};

// an empty file is the end at once
TEST_F(log_entry_reader_test, empty_file_is_end) {
    auto p = write_file("");
    log_entry_reader reader(p);
    expect_end(reader, 0);
}

// reads the entries in order with their positions, and is the end when the file ends at a boundary; returns the end
// again after the end
TEST_F(log_entry_reader_test, reads_entries_in_order) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    std::string e3 = remove_entry(1, "key", 7, 2);
    auto p = write_file(e1 + e2 + e3);
    log_entry_reader reader(p);
    EXPECT_EQ(reader.file(), p);
    expect_entry(reader, e1, 0);
    expect_entry(reader, e2, e1.size());
    expect_entry(reader, e3, e1.size() + e2.size());
    expect_end(reader, e1.size() + e2.size() + e3.size());
    expect_end(reader, e1.size() + e2.size() + e3.size());
}

// a file cut in the middle of an entry is short_entry, distinguished from the end; after that, returns end at the
// size of the file
TEST_F(log_entry_reader_test, truncated_entry_is_short_entry) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    for (std::size_t cut = 1; cut < e2.size(); cut++) {
        auto p = write_file(e1 + e2.substr(0, cut));
        log_entry_reader reader(p);
        expect_entry(reader, e1, 0);
        expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
        expect_end(reader, e1.size() + cut);
        if (HasFailure()) {
            FAIL() << "cut " << cut;
        }
    }
}

// an invalid entry type is unknown_type, with the value of the type byte
TEST_F(log_entry_reader_test, invalid_type_is_unknown_type) {
    std::string e1 = marker(2, 7);
    for (std::uint8_t type_value : {0, 99}) {
        auto p = write_file(e1 + u8(type_value) + "garbage");
        log_entry_reader reader(p);
        expect_entry(reader, e1, 0);
        expect_error(reader, log_entry::read_error::unknown_type, static_cast<entry_type>(type_value), e1.size());
        expect_error(reader, log_entry::read_error::unknown_type, static_cast<entry_type>(type_value), e1.size());
    }
}

// starts reading at the given position; starting at the size of the file is the end
TEST_F(log_entry_reader_test, starts_at_offset) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    auto p = write_file(e1 + e2);
    {
        log_entry_reader reader(p, e1.size());
        expect_entry(reader, e2, e1.size());
        expect_end(reader, e1.size() + e2.size());
    }
    {
        log_entry_reader reader(p, e1.size() + e2.size());
        expect_end(reader, e1.size() + e2.size());
    }
}

// throws an exception without errno if the start position exceeds the size of the file
TEST_F(log_entry_reader_test, start_offset_beyond_file_size_throws) {
    std::string e1 = marker(2, 7);
    auto p = write_file(e1);
    try {
        log_entry_reader reader(p, e1.size() + 1);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), 0);
    }
}

// throws an exception with errno if the file cannot be opened
TEST_F(log_entry_reader_test, open_failure_throws) {
    try {
        log_entry_reader reader(boost::filesystem::path(location) / "not_exist");
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
    }
}

// throws an exception with errno if the file status cannot be obtained
TEST_F(log_entry_reader_test, fstat_failure_throws) {
    class : public real_file_operations {
    public:
        int fstat(int, struct stat*) override {
            errno = EIO;
            return -1;
        }
    } ops;
    auto p = write_file(marker(2, 7));
    try {
        log_entry_reader reader(p, 0, nullptr, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// throws an exception with errno if reading fails; retries on EINTR
TEST_F(log_entry_reader_test, read_failure_throws_and_eintr_retries) {
    class : public real_file_operations {
    public:
        ssize_t pread(int fd, void* buf, size_t count, off_t offset) override {
            calls++;
            if (calls == 1) {
                errno = EINTR;
                return -1;
            }
            if (calls == 2) {
                return real_file_operations::pread(fd, buf, count, offset);
            }
            errno = EIO;
            return -1;
        }
        int calls = 0;
    } ops;
    // the first read (after EINTR) reads e1 and part of e2, and the second read fails
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", std::string(buffer_size, 'v'), 7, 1);
    auto p = write_file(e1 + e2);
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, e1, 0);
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// gives the same results when a read returns fewer bytes than requested
TEST_F(log_entry_reader_test, short_reads) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    std::string e3 = remove_entry(1, "key", 7, 2);
    auto p = write_file(e1 + e2 + e3 + e2.substr(0, 5));
    for (std::size_t max_read : {1, 2, 3, 7}) {
        recording_file_operations ops(max_read);
        log_entry_reader reader(p, 0, nullptr, ops);
        expect_entry(reader, e1, 0);
        expect_entry(reader, e2, e1.size());
        expect_entry(reader, e3, e1.size() + e2.size());
        expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry,
                     e1.size() + e2.size() + e3.size());
        if (HasFailure()) {
            FAIL() << "max_read " << max_read;
        }
    }
}

// even if a length field is broken and unread bytes remain, returns end at the size of the file after short_entry,
// without parsing the remaining bytes
TEST_F(log_entry_reader_test, short_entry_skips_unread_bytes) {
    std::string broken = u8(1) + u32le(0xffffffffU) + u32le(0xffffffffU) + std::string(buffer_size + 100, 'x');
    auto p = write_file(broken);
    log_entry_reader reader(p);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, 0);
    expect_end(reader, broken.size());
}

// returns short_entry without expanding the buffer if a length field exceeds the rest of the file
TEST_F(log_entry_reader_test, length_beyond_file_does_not_allocate) {
    std::string broken = u8(1) + u32le(0xffffffffU) + u32le(0xffffffffU) + "rest";
    auto p = write_file(broken);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, 0);
    for (auto count : ops.requests()) {
        EXPECT_LE(count, buffer_size);
    }
}

// refill branches: expands for an entry larger than the buffer size, and shrinks to the buffer size for a small
// entry after it
TEST_F(log_entry_reader_test, buffer_expands_and_shrinks) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    std::string small = remove_entry(1, "key", 7, 2);
    auto p = write_file(large + small);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, large, 0);
    expect_entry(reader, small, large.size());
    expect_end(reader, large.size() + small.size());
    // 1st: reads the buffer size. 2nd: expands to the size of the entry, and reads the rest.
    // 3rd: reads the whole free space of the buffer shrunk to the buffer size (without shrinking, the free space would
    // be the size of the entry)
    ASSERT_GE(ops.requests().size(), 3U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], large.size() - buffer_size);
    EXPECT_EQ(ops.requests()[2], buffer_size);
}

// refill branches: for an entry across the boundary of the buffer size, moves the unparsed part to the beginning
TEST_F(log_entry_reader_test, buffer_moves_unparsed_to_front) {
    // the first entry fills up to 10 bytes before the boundary, so that the second entry crosses the boundary
    std::string first = normal_entry(1, "key", "", 7, 1);
    first = normal_entry(1, "key", std::string(buffer_size - 10 - first.size(), 'v'), 7, 1);
    std::string second = normal_entry(1, "key", "value", 7, 2);
    auto p = write_file(first + second);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, first, 0);
    expect_entry(reader, second, first.size());
    expect_end(reader, first.size() + second.size());
    // 2nd: moves the unparsed 10 bytes to the beginning, and reads the rest of the buffer
    ASSERT_GE(ops.requests().size(), 2U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], buffer_size - 10);
}

// returns short_entry without expanding the buffer if an entry larger than the buffer size is cut in the middle
TEST_F(log_entry_reader_test, truncated_large_entry_does_not_expand) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    auto p = write_file(large.substr(0, large.size() - 50));
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, 0);
    for (auto count : ops.requests()) {
        EXPECT_LE(count, buffer_size);
    }
}

// normal_with_blob expands twice: up to the BLOB count field, and to the size of the entry
TEST_F(log_entry_reader_test, buffer_expands_twice_for_blob_ids) {
    std::vector<std::uint64_t> blob_ids{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::string large = normal_with_blob(1, "key", std::string(buffer_size, 'v'), 7, 1, blob_ids);
    auto p = write_file(large);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, large, 0);
    expect_end(reader, large.size());
    // 1st: reads the buffer size. 2nd: expands up to the end of the BLOB count field, and reads the rest.
    // 3rd: expands to the size of the entry, and reads the BLOB IDs
    const std::size_t through_blob_count = large.size() - blob_ids.size() * sizeof(std::uint64_t);
    ASSERT_GE(ops.requests().size(), 3U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], through_blob_count - buffer_size);
    EXPECT_EQ(ops.requests()[2], large.size() - through_blob_count);
}

// gives the same results when reads after the expansion return fewer bytes than requested
TEST_F(log_entry_reader_test, short_reads_after_expansion) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    std::string small = remove_entry(1, "key", 7, 2);
    auto p = write_file(large + small);
    // an amount that does not divide the buffer size, so that read boundaries differ from entry boundaries
    recording_file_operations ops(1000003);
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, large, 0);
    expect_entry(reader, small, large.size());
    expect_end(reader, large.size() + small.size());
}

// for an entry that starts in the middle of the buffer and does not fit in the buffer size, moves the unparsed part
// to the beginning of the expanded buffer
TEST_F(log_entry_reader_test, buffer_expands_from_middle) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    std::string e3 = remove_entry(1, "key", 7, 2);
    std::string smalls = e1 + e2 + e3;
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 3);
    auto p = write_file(smalls + large);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    expect_entry(reader, e1, 0);
    expect_entry(reader, e2, e1.size());
    expect_entry(reader, e3, e1.size() + e2.size());
    expect_entry(reader, large, smalls.size());
    expect_end(reader, smalls.size() + large.size());
    // 2nd: moves the head of `large` read by the 1st read to the beginning of the expanded buffer, and reads the rest
    ASSERT_GE(ops.requests().size(), 2U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], large.size() - (buffer_size - smalls.size()));
}

// when the file becomes shorter after it is opened and pread returns 0, the result is end at an entry boundary and
// short_entry in the middle of an entry. This checks the defense outside the assumption that the file does not change
TEST_F(log_entry_reader_test, file_truncated_after_open) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    {
        auto p = write_file(e1 + e2);
        log_entry_reader reader(p);
        boost::filesystem::resize_file(p, e1.size());
        expect_entry(reader, e1, 0);
        expect_end(reader, e1.size());
    }
    {
        auto p = write_file(e1 + e2);
        log_entry_reader reader(p);
        boost::filesystem::resize_file(p, e1.size() + 5);
        expect_entry(reader, e1, 0);
        expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
    }
}

// one step of writing a file being written: the bytes to append, and the state the state function returns after
// appending
struct write_step {
    std::string bytes;
    file_write_state state;
};

// advances one step each time it is called: appends to the file and returns the state. Returns written after all the
// steps are used. Since it is called where the reader waits, it reproduces "more data arrives while waiting" without a
// writer thread
class scripted_writer {
public:
    scripted_writer(boost::filesystem::path file, std::vector<write_step> steps)
        : file_(std::move(file)), steps_(std::move(steps)) {}

    file_write_state step() {
        calls_++;
        if (next_ >= steps_.size()) {
            return file_write_state::written;
        }
        const auto& s = steps_[next_++];
        if (! s.bytes.empty()) {
            std::ofstream out(file_.string(), std::ios::binary | std::ios::app);
            out.write(s.bytes.data(), static_cast<std::streamsize>(s.bytes.size()));
        }
        return s.state;
    }

    [[nodiscard]] std::size_t calls() const noexcept { return calls_; }

    [[nodiscard]] file_write_state_function function() {
        return [this]() { return step(); };
    }

private:
    boost::filesystem::path file_;
    std::vector<write_step> steps_;
    std::size_t next_{};
    std::size_t calls_{};
};

// while being written, waits in the middle of an entry and parses it after the rest arrives; after written, a file
// ending at a boundary is the end
TEST_F(log_entry_reader_test, writing_waits_until_written) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    auto p = write_file(e1 + e2.substr(0, 5));
    scripted_writer writer(p, {{e2.substr(5), file_write_state::writing}});
    log_entry_reader reader(p, 0, writer.function());
    expect_entry(reader, e1, 0);
    expect_entry(reader, e2, e1.size());
    expect_end(reader, e1.size() + e2.size());
    EXPECT_EQ(writer.calls(), 2U);
}

// after written, a file ending in the middle of an entry is short_entry
TEST_F(log_entry_reader_test, written_then_short_entry) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    auto p = write_file(e1 + e2.substr(0, 5));
    scripted_writer writer(p, {});
    log_entry_reader reader(p, 0, writer.function());
    expect_entry(reader, e1, 0);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
}

// throws an exception without errno when the state becomes failed
TEST_F(log_entry_reader_test, writing_failed_throws) {
    std::string e1 = marker(2, 7);
    auto p = write_file(e1);
    scripted_writer writer(p, {{"", file_write_state::writing}, {"", file_write_state::failed}});
    log_entry_reader reader(p, 0, writer.function());
    expect_entry(reader, e1, 0);
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), 0);
    }
    EXPECT_EQ(writer.calls(), 2U);
}

// after a failure, throws without calling the state function, even if it would return writing
TEST_F(log_entry_reader_test, failed_is_final) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    auto p = write_file(e1);
    scripted_writer writer(p, {{"", file_write_state::failed}, {e2, file_write_state::writing}});
    log_entry_reader reader(p, 0, writer.function());
    expect_entry(reader, e1, 0);
    for (int i = 0; i < 2; i++) {
        try {
            (void) reader.next();
            FAIL() << "no exception " << i;
        } catch (const limestone_io_exception& e) {
            EXPECT_EQ(e.error_code(), 0);
        }
    }
    EXPECT_EQ(writer.calls(), 1U);
}

// if a file being written does not exist, does not throw at creation, and waits for it to appear
TEST_F(log_entry_reader_test, writing_waits_for_file_to_appear) {
    std::string e1 = marker(2, 7);
    boost::filesystem::path p = boost::filesystem::path(location) / "data";
    scripted_writer writer(p, {{"", file_write_state::writing}, {e1, file_write_state::writing}});
    log_entry_reader reader(p, 0, writer.function());
    expect_entry(reader, e1, 0);
    expect_end(reader, e1.size());
}

// throws an exception with errno if the file does not exist after written; a later call throws again, not end
TEST_F(log_entry_reader_test, written_without_file_throws) {
    boost::filesystem::path p = boost::filesystem::path(location) / "data";
    scripted_writer writer(p, {});
    log_entry_reader reader(p, 0, writer.function());
    for (int i = 0; i < 2; i++) {
        try {
            (void) reader.next();
            FAIL() << "no exception " << i;
        } catch (const limestone_io_exception& e) {
            EXPECT_EQ(e.error_code(), ENOENT);
        }
    }
}

// for a file being written, waits even if the start position exceeds the size of the file
TEST_F(log_entry_reader_test, writing_waits_for_start_offset) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    std::string e3 = remove_entry(1, "key", 7, 2);
    auto p = write_file(e1);
    scripted_writer writer(p, {{e2 + e3, file_write_state::writing}});
    log_entry_reader reader(p, e1.size() + e2.size(), writer.function());
    expect_entry(reader, e3, e1.size() + e2.size());
    expect_end(reader, e1.size() + e2.size() + e3.size());
}

// throws an exception without errno if the start position still exceeds the size of the file after written
TEST_F(log_entry_reader_test, written_with_start_offset_beyond_file_size_throws) {
    std::string e1 = marker(2, 7);
    auto p = write_file(e1);
    scripted_writer writer(p, {});
    log_entry_reader reader(p, e1.size() + 10, writer.function());
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), 0);
    }
}

// throws an exception without errno if, when written, the file is smaller than the range already read
TEST_F(log_entry_reader_test, written_shorter_than_read_throws) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    auto p = write_file(e1 + e2);
    log_entry_reader reader(p, 0, [&p, &e1]() {
        boost::filesystem::resize_file(p, e1.size());
        return file_write_state::written;
    });
    expect_entry(reader, e1, 0);
    expect_entry(reader, e2, e1.size());
    for (int i = 0; i < 2; i++) {
        try {
            (void) reader.next();
            FAIL() << "no exception " << i;
        } catch (const limestone_io_exception& e) {
            EXPECT_EQ(e.error_code(), 0);
        }
    }
}

// while the lower bound exceeds the buffer, waits without allocating or reading until the rest of the file reaches
// the lower bound
TEST_F(log_entry_reader_test, writing_does_not_allocate_while_waiting) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    auto p = write_file(large.substr(0, 100));
    constexpr std::size_t middle = 10U * 1024U * 1024U;
    scripted_writer writer(p, {{large.substr(100, middle - 100), file_write_state::writing},
                               {large.substr(middle, large.size() - 1 - middle), file_write_state::writing},
                               {large.substr(large.size() - 1), file_write_state::writing}});
    recording_file_operations ops;
    log_entry_reader reader(p, 0, writer.function(), ops);
    expect_entry(reader, large, 0);
    expect_end(reader, large.size());
    // 1st: reads the buffer size (only 100 bytes have arrived). No read while waiting.
    // 2nd: after everything arrives, expands to the size of the entry and reads the rest. 3rd: reads into the buffer
    // shrunk to the buffer size
    ASSERT_EQ(ops.requests().size(), 3U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], large.size() - 100);
    EXPECT_EQ(ops.requests()[2], buffer_size);
    EXPECT_EQ(writer.calls(), 4U);
}

// if the state becomes written while waiting with the lower bound beyond the buffer, and the file ends in the middle
// of an entry, returns short_entry without allocating
TEST_F(log_entry_reader_test, written_while_waiting_then_short_entry) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    auto p = write_file(large.substr(0, 100));
    scripted_writer writer(p, {{large.substr(100, 1000), file_write_state::writing}});
    recording_file_operations ops;
    log_entry_reader reader(p, 0, writer.function(), ops);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, 0);
    for (auto count : ops.requests()) {
        EXPECT_LE(count, buffer_size);
    }
}

// throws an exception with errno if a file being written cannot be opened for a reason other than not existing
TEST_F(log_entry_reader_test, writing_open_failure_throws) {
    class : public real_file_operations {
    public:
        int open(const char*, int) override {
            errno = EACCES;
            return -1;
        }
    } ops;
    auto p = write_file(marker(2, 7));
    scripted_writer writer(p, {});
    try {
        log_entry_reader reader(p, 0, writer.function(), ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EACCES);
    }
}

// throws an exception with errno if the size of a file being written cannot be obtained again
TEST_F(log_entry_reader_test, writing_fstat_failure_throws) {
    class : public real_file_operations {
    public:
        int fstat(int, struct stat*) override {
            errno = EIO;
            return -1;
        }
    } ops;
    auto p = write_file(marker(2, 7));
    scripted_writer writer(p, {});
    log_entry_reader reader(p, 0, writer.function(), ops);
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// a reader of a file being written cannot be created with an empty state function
TEST_F(log_entry_reader_test, empty_write_state_function_aborts) {
    auto p = write_file(marker(2, 7));
    EXPECT_DEATH(log_entry_reader(p, 0, file_write_state_function{}), "empty write state function");
}

// a result of kind ok cannot be created from a default-constructed view
TEST_F(log_entry_reader_test, read_result_ok_with_default_view_aborts) {
    EXPECT_DEATH((void) log_entry_read_result::ok(log_entry_view{}, 0),
                 "log_entry_read_result:ok default-constructed view");
}

// asking for a value not valid for the result kind aborts; offset() is valid for all kinds
TEST_F(log_entry_reader_test, read_result_accessor_validity) {
    auto p = write_file(marker(2, 7));
    log_entry_reader reader(p);
    auto ok = reader.next();
    ASSERT_EQ(ok.result(), status::ok);
    EXPECT_EQ(ok.offset(), 0U);
    EXPECT_DEATH((void) ok.error(), "log_entry_read_result:error not valid");

    auto end = log_entry_read_result::end(9);
    EXPECT_EQ(end.offset(), 9U);
    EXPECT_DEATH((void) end.view(), "log_entry_read_result:view not valid");
    EXPECT_DEATH((void) end.error(), "log_entry_read_result:error not valid");

    auto error = log_entry_read_result::error(log_entry::read_error{log_entry::read_error::short_entry,
                                                                    entry_type::normal_entry},
                                              3, 9);
    EXPECT_EQ(error.offset(), 3U);
    EXPECT_EQ(error.required(), 9U);
    EXPECT_DEATH((void) error.view(), "log_entry_read_result:view not valid");

    // required() は short_entry のときだけ有効
    EXPECT_DEATH((void) ok.required(), "log_entry_read_result:required not valid");
    EXPECT_DEATH((void) end.required(), "log_entry_read_result:required not valid");
    auto unknown = log_entry_read_result::error(log_entry::read_error{log_entry::read_error::unknown_type,
                                                                      static_cast<entry_type>(99)},
                                                3, 0);
    EXPECT_DEATH((void) unknown.required(), "log_entry_read_result:required not valid");
}

// the string representation of the result kinds
TEST_F(log_entry_reader_test, status_to_string) {
    std::ostringstream out;
    out << status::ok << ' ' << status::end << ' ' << status::error << ' ' << static_cast<status>(99);
    EXPECT_EQ(out.str(), "ok end error unknown");
}

// システムコールの呼び出しを数えるファイル操作
class counting_file_operations : public real_file_operations {
public:
    int open(const char* filename, int flags) override {
        ++others_;
        return real_file_operations::open(filename, flags);
    }
    int close(int fd) override {
        ++others_;
        return real_file_operations::close(fd);
    }
    int fstat(int fd, struct stat* buf) override {
        ++others_;
        return real_file_operations::fstat(fd, buf);
    }
    ssize_t pread(int fd, void* buf, size_t count, off_t offset) override {
        ++preads_;
        return real_file_operations::pread(fd, buf, count, offset);
    }

    // pread 以外のシステムコールの回数
    [[nodiscard]] std::size_t others() const noexcept { return others_; }
    // pread の回数
    [[nodiscard]] std::size_t preads() const noexcept { return preads_; }

private:
    std::size_t others_{};
    std::size_t preads_{};
};

// バッファの大きさを指定して開いた reader に範囲を設定して読める。範囲の設定では、システムコールを呼ばない
TEST_F(log_entry_reader_test, reads_the_range_set) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    std::string e2 = remove_entry(1, "b", 7, 2);
    std::string e3 = normal_entry(1, "c", "v", 7, 3);
    auto p = write_file(marker(2, 7) + e1 + e2 + e3);
    counting_file_operations ops;
    {
        log_entry_reader reader(p, 0, 4096, ops);
        EXPECT_EQ(reader.file(), p);
        // 構築では、開く操作とファイルの状態の取得だけを呼ぶ
        EXPECT_EQ(ops.others(), 2U);
        EXPECT_EQ(ops.preads(), 0U);
        reader.set_range(9, 9 + e1.size() + e2.size());
        EXPECT_EQ(ops.others(), 2U);
        EXPECT_EQ(ops.preads(), 0U);
        expect_entry(reader, e1, 9);
        expect_entry(reader, e2, 9 + e1.size());
        expect_end(reader, 9 + e1.size() + e2.size());
        EXPECT_EQ(ops.others(), 2U);
        EXPECT_EQ(ops.preads(), 1U);
    }
    // 破棄では、閉じる操作だけを呼ぶ
    EXPECT_EQ(ops.others(), 3U);
}

// 範囲を設定しなければ、バッファの大きさを指定しない reader と同じく、開始位置からファイルの末尾まで読む
TEST_F(log_entry_reader_test, reads_to_the_end_without_range) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    std::string e2 = remove_entry(1, "b", 7, 2);
    auto p = write_file(marker(2, 7) + e1 + e2);
    log_entry_reader reader(p, 9, 4096);
    expect_entry(reader, e1, 9);
    expect_entry(reader, e2, 9 + e1.size());
    expect_end(reader, 9 + e1.size() + e2.size());
}

// 開いた後にファイルの名前を削除しても、範囲を設定して読める
TEST_F(log_entry_reader_test, reads_the_range_after_unlink) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    auto p = write_file(e1);
    log_entry_reader reader(p, 0, 4096);
    boost::filesystem::remove(p);
    reader.set_range(0, e1.size());
    expect_entry(reader, e1, 0);
    expect_end(reader, e1.size());
}

// 範囲を設定し直すと、その範囲を先頭から読む。前の範囲の途中でも、手前の位置へも設定し直せる
TEST_F(log_entry_reader_test, set_range_switches_the_range) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    std::string e2 = normal_entry(1, "b", "v", 7, 2);
    std::string e3 = normal_entry(1, "c", "v", 7, 3);
    auto p = write_file(e1 + e2 + e3);
    log_entry_reader reader(p, 0, 4096);
    const std::uint64_t o2 = e1.size();
    const std::uint64_t o3 = e1.size() + e2.size();
    const std::uint64_t size = o3 + e3.size();
    // 後ろの範囲を最後まで読む
    reader.set_range(o3, size);
    expect_entry(reader, e3, o3);
    expect_end(reader, size);
    // 手前の範囲に設定し直し、途中まで読む
    reader.set_range(0, o3);
    expect_entry(reader, e1, 0);
    // 読み終える前に設定し直す。前の範囲の残りは読まれず、新しい範囲の先頭から読む
    reader.set_range(o2, size);
    expect_entry(reader, e2, o2);
    expect_entry(reader, e3, o3);
    expect_end(reader, size);
}

// バッファの拡張と縮小は、コンストラクタで指定した大きさを基準にする
TEST_F(log_entry_reader_test, buffer_size_given_to_the_constructor) {
    constexpr std::size_t size = 4096;
    std::string large = normal_entry(1, "key", std::string(size + 100, 'v'), 7, 1);
    std::string small = remove_entry(1, "key", 7, 2);
    auto p = write_file(large + small);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, size, ops);
    reader.set_range(0, large.size() + small.size());
    expect_entry(reader, large, 0);
    expect_entry(reader, small, large.size());
    expect_end(reader, large.size() + small.size());
    // 1 回目は指定した大きさ、2 回目はエントリの大きさに拡張してその残り、3 回目は指定した大きさに縮小した
    // バッファで、読み終える位置までの残り
    ASSERT_EQ(ops.requests().size(), 3U);
    EXPECT_EQ(ops.requests()[0], size);
    EXPECT_EQ(ops.requests()[1], large.size() - size);
    EXPECT_EQ(ops.requests()[2], small.size());
    // 設定し直しても同じバッファで読む
    reader.set_range(large.size(), large.size() + small.size());
    expect_entry(reader, small, large.size());
    ASSERT_EQ(ops.requests().size(), 4U);
    EXPECT_EQ(ops.requests()[3], small.size());
}

// 読み終える位置がエントリの境界なら、そこで end になり、その後ろは読まない (要求したバイト数の合計が範囲の大きさ)
TEST_F(log_entry_reader_test, end_offset_at_entry_boundary) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    std::string e2 = normal_entry(1, "b", std::string(100, 'v'), 7, 2);
    std::string e3 = normal_entry(1, "c", std::string(100, 'v'), 7, 3);
    std::string e4 = normal_entry(1, "d", "v", 7, 4);
    auto p = write_file(e1 + e2 + e3 + e4);
    recording_file_operations ops;
    const std::uint64_t start_offset = e1.size();
    const std::uint64_t end_offset = e1.size() + e2.size() + e3.size();
    // バッファの大きさが範囲より小さくても、読み込みは範囲の残りまでに限る
    log_entry_reader reader(p, 0, 64, ops);
    reader.set_range(start_offset, end_offset);
    expect_entry(reader, e2, start_offset);
    expect_entry(reader, e3, start_offset + e2.size());
    expect_end(reader, end_offset);
    expect_end(reader, end_offset);
    std::size_t requested = 0;
    for (std::size_t r : ops.requests()) {
        requested += r;
    }
    EXPECT_EQ(requested, end_offset - start_offset);
}

// 開始位置と読み終える位置が同じなら、何も読まずに end になる
TEST_F(log_entry_reader_test, empty_range_is_end) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    auto p = write_file(e1 + e1);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, 4096, ops);
    reader.set_range(e1.size(), e1.size());
    expect_end(reader, e1.size());
    EXPECT_TRUE(ops.requests().empty());
}

// 読み終える位置がエントリの途中なら、そのエントリは short_entry になり、次は読み終える位置で end になる
TEST_F(log_entry_reader_test, end_offset_in_the_middle_of_entry) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    std::string e2 = normal_entry(1, "b", "v", 7, 2);
    auto p = write_file(e1 + e2);
    const std::uint64_t end_offset = e1.size() + 5;
    log_entry_reader reader(p, 0, 4096);
    reader.set_range(0, end_offset);
    expect_entry(reader, e1, 0);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
    expect_end(reader, end_offset);
    // エラーの後でも設定し直せる
    reader.set_range(e1.size(), e1.size() + e2.size());
    expect_entry(reader, e2, e1.size());
}

// short_entry の結果は解析に必要なバイト数の下限 (required) を持つ。読み終える位置を required まで広げるのを繰り返すと
// 読める (normal_entry はヘッダの次で全体の長さが確定し、normal_with_blob は blob_count の次でもう 1 段ある)
TEST_F(log_entry_reader_test, short_entry_has_required) {
    std::string e1 = normal_entry(1, "key", std::string(100, 'v'), 7, 1);
    std::string e2 = normal_with_blob(1, "key", std::string(100, 'v'), 7, 2, {11, 12});
    auto p = write_file(e1 + e2);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, 4096, ops);

    // normal_entry: 5 バイト → ヘッダ (9 バイト) → 全体
    reader.set_range(0, 5);
    auto r1 = reader.next();
    ASSERT_EQ(r1.result(), status::error);
    ASSERT_EQ(r1.error().value(), log_entry::read_error::short_entry);
    EXPECT_EQ(r1.offset(), 0U);
    EXPECT_EQ(r1.required(), 9U);
    reader.set_range(0, r1.required());
    auto r2 = reader.next();
    ASSERT_EQ(r2.result(), status::error);
    ASSERT_EQ(r2.error().value(), log_entry::read_error::short_entry);
    EXPECT_EQ(r2.required(), e1.size());
    reader.set_range(0, r2.required());
    expect_entry(reader, e1, 0);

    // normal_with_blob: ヘッダ → blob_count まで → 全体
    reader.set_range(e1.size(), e1.size() + 9);
    auto r3 = reader.next();
    ASSERT_EQ(r3.result(), status::error);
    ASSERT_EQ(r3.error().value(), log_entry::read_error::short_entry);
    EXPECT_EQ(r3.offset(), e1.size());
    EXPECT_EQ(r3.required(), e2.size() - 2 * 8);
    reader.set_range(e1.size(), e1.size() + r3.required());
    auto r4 = reader.next();
    ASSERT_EQ(r4.result(), status::error);
    ASSERT_EQ(r4.error().value(), log_entry::read_error::short_entry);
    EXPECT_EQ(r4.required(), e2.size());
    reader.set_range(e1.size(), e1.size() + r4.required());
    expect_entry(reader, e2, e1.size());

    // 読み込みは毎回、設定した範囲の大きさだけ
    const std::vector<std::size_t> expected{5, 9, e1.size(), 9, e2.size() - 2 * 8, e2.size()};
    EXPECT_EQ(ops.requests(), expected);
}

// ファイルがエントリの途中で終わっているときの short_entry も required を持ち、それはファイルの残りより大きい
TEST_F(log_entry_reader_test, short_entry_at_file_end_has_required) {
    std::string e1 = normal_entry(1, "key", std::string(100, 'v'), 7, 1);
    auto p = write_file(e1.substr(0, 20));
    log_entry_reader reader(p, 0, 4096);
    auto r = reader.next();
    ASSERT_EQ(r.result(), status::error);
    ASSERT_EQ(r.error().value(), log_entry::read_error::short_entry);
    EXPECT_EQ(r.offset(), 0U);
    EXPECT_EQ(r.required(), e1.size());
    expect_end(reader, 20);
}

// 範囲がファイルの大きさに収まらなくても例外にはならず、読み込みが尽きた位置で end になる
TEST_F(log_entry_reader_test, range_beyond_the_file_ends_at_the_file_size) {
    std::string e1 = normal_entry(1, "a", "v", 7, 1);
    auto p = write_file(e1);
    log_entry_reader reader(p, 0, 4096);
    reader.set_range(0, e1.size() + 100);
    expect_entry(reader, e1, 0);
    expect_end(reader, e1.size());
    reader.set_range(e1.size() + 10, e1.size() + 100);
    expect_end(reader, e1.size() + 10);
}

// 書き込み中のファイルを読む reader では set_range は abort する。開始位置が読み終える位置を超えても abort する。
// バッファの大きさ 0 も abort する
TEST_F(log_entry_reader_test, invalid_use_of_set_range_aborts) {
    auto p = write_file(marker(2, 7));
    EXPECT_DEATH(
        {
            log_entry_reader reader(p, 0, [] { return file_write_state::written; });
            reader.set_range(0, 9);
        },
        "set_range is not available");
    EXPECT_DEATH(
        {
            log_entry_reader reader(p, 0, 4096);
            reader.set_range(2, 1);
        },
        "start offset 2 exceeds end offset 1");
    EXPECT_DEATH(log_entry_reader(p, 0, 0), "buffer size is 0");
}

}  // namespace limestone::testing
