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

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include "file_operations.h"
#include "log_entry/log_entry_reader.h"
#include "log_entry_bytes.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::api::log_entry;
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

constexpr std::size_t buffer_size = log_entry_reader::buffer_size;

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
    expect_entry(reader, e1, 0);
    expect_entry(reader, e2, e1.size());
    expect_entry(reader, e3, e1.size() + e2.size());
    expect_end(reader, e1.size() + e2.size() + e3.size());
    expect_end(reader, e1.size() + e2.size() + e3.size());
}

// a file cut in the middle of an entry is short_entry, distinguished from the end; returns the same result again
TEST_F(log_entry_reader_test, truncated_entry_is_short_entry) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    for (std::size_t cut = 1; cut < e2.size(); cut++) {
        auto p = write_file(e1 + e2.substr(0, cut));
        log_entry_reader reader(p);
        expect_entry(reader, e1, 0);
        expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
        expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, e1.size());
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
        log_entry_reader reader(p, 0, ops);
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
    log_entry_reader reader(p, 0, ops);
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
        log_entry_reader reader(p, 0, ops);
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

// returns short_entry without expanding the buffer if a length field exceeds the rest of the file
TEST_F(log_entry_reader_test, length_beyond_file_does_not_allocate) {
    std::string broken = u8(1) + u32le(0xffffffffU) + u32le(0xffffffffU) + "rest";
    auto p = write_file(broken);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, ops);
    expect_error(reader, log_entry::read_error::short_entry, entry_type::normal_entry, 0);
    for (auto count : ops.requests()) {
        EXPECT_LE(count, buffer_size);
    }
}

// refill branches: expands for an entry larger than the normal size, and shrinks to the normal size for a small
// entry after it
TEST_F(log_entry_reader_test, buffer_expands_and_shrinks) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    std::string small = remove_entry(1, "key", 7, 2);
    auto p = write_file(large + small);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, ops);
    expect_entry(reader, large, 0);
    expect_entry(reader, small, large.size());
    expect_end(reader, large.size() + small.size());
    // 1st: reads the normal size. 2nd: expands to the size of the entry, and reads the rest.
    // 3rd: reads the whole free space of the buffer shrunk to the normal size (without shrinking, the free space would
    // be the size of the entry)
    ASSERT_GE(ops.requests().size(), 3U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], large.size() - buffer_size);
    EXPECT_EQ(ops.requests()[2], buffer_size);
}

// refill branches: for an entry across the boundary of the normal size, moves the unparsed part to the beginning
TEST_F(log_entry_reader_test, buffer_moves_unparsed_to_front) {
    // the first entry fills up to 10 bytes before the boundary, so that the second entry crosses the boundary
    std::string first = normal_entry(1, "key", "", 7, 1);
    first = normal_entry(1, "key", std::string(buffer_size - 10 - first.size(), 'v'), 7, 1);
    std::string second = normal_entry(1, "key", "value", 7, 2);
    auto p = write_file(first + second);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, ops);
    expect_entry(reader, first, 0);
    expect_entry(reader, second, first.size());
    expect_end(reader, first.size() + second.size());
    // 2nd: moves the unparsed 10 bytes to the beginning, and reads the rest of the normal-size buffer
    ASSERT_GE(ops.requests().size(), 2U);
    EXPECT_EQ(ops.requests()[0], buffer_size);
    EXPECT_EQ(ops.requests()[1], buffer_size - 10);
}

// returns short_entry without expanding the buffer if an entry larger than the normal size is cut in the middle
TEST_F(log_entry_reader_test, truncated_large_entry_does_not_expand) {
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 1);
    auto p = write_file(large.substr(0, large.size() - 50));
    recording_file_operations ops;
    log_entry_reader reader(p, 0, ops);
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
    log_entry_reader reader(p, 0, ops);
    expect_entry(reader, large, 0);
    expect_end(reader, large.size());
    // 1st: reads the normal size. 2nd: expands up to the end of the BLOB count field, and reads the rest.
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
    // an amount that does not divide the normal size, so that read boundaries differ from entry boundaries
    recording_file_operations ops(1000003);
    log_entry_reader reader(p, 0, ops);
    expect_entry(reader, large, 0);
    expect_entry(reader, small, large.size());
    expect_end(reader, large.size() + small.size());
}

// for an entry that starts in the middle of the buffer and does not fit in the normal size, moves the unparsed part
// to the beginning of the expanded buffer
TEST_F(log_entry_reader_test, buffer_expands_from_middle) {
    std::string e1 = marker(2, 7);
    std::string e2 = normal_entry(1, "key", "value", 7, 1);
    std::string e3 = remove_entry(1, "key", 7, 2);
    std::string smalls = e1 + e2 + e3;
    std::string large = normal_entry(1, "key", std::string(buffer_size + 100, 'v'), 7, 3);
    auto p = write_file(smalls + large);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, ops);
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
                                              3);
    EXPECT_EQ(error.offset(), 3U);
    EXPECT_DEATH((void) error.view(), "log_entry_read_result:view not valid");
}

// the string representation of the result kinds
TEST_F(log_entry_reader_test, status_to_string) {
    std::ostringstream out;
    out << status::ok << ' ' << status::end << ' ' << status::error << ' ' << static_cast<status>(99);
    EXPECT_EQ(out.str(), "ok end error unknown");
}

}  // namespace limestone::testing
