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
#include <cerrno>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "sort/sort_buffer.h"
#include "sort/sorted_file_reader.h"
#include "sort/sorted_file_writer.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::log_entry_view;
using limestone::internal::sort_buffer;
using limestone::internal::sorted_file_reader;
using limestone::internal::sorted_file_writer;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;

namespace {

/**
 * @brief Parses the bytes of one entry and creates a view.
 * @param bytes the bytes of one entry. Fails the test if parsing does not succeed
 * @return the view
 */
log_entry_view view_of(std::string_view bytes) {
    auto r = log_entry_view::parse(bytes);
    EXPECT_TRUE(r) << "parse failed";
    return r.view();
}

/**
 * @brief Reads the whole contents of a file.
 * @param p the file
 * @return the contents
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

class sorted_file_writer_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sorted_file_writer_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path file() {
        return boost::filesystem::path(location) / "sorted";
    }
};

// entries of the three types passed in ascending order of key_sid are written as they are (no marker, write_version
// unchanged, remove_entry kept)
TEST_F(sorted_file_writer_test, writes_entries_as_they_are) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = normal_with_blob(1, "b", "v", 11, 2, {100, 200});
    const std::string e3 = remove_entry(1, "c", 12, 3);

    sorted_file_writer writer(file());
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    EXPECT_EQ(writer.written_count(), 3U);
    writer.close();

    EXPECT_EQ(read_file(file()), e1 + e2 + e3);
}

// only the first of the entries with the same key_sid is written
TEST_F(sorted_file_writer_test, deduplicates_same_key_sid) {
    const std::string a_new = normal_entry(1, "a", "new", 2, 0);
    const std::string a_old = normal_entry(1, "a", "old", 1, 0);
    const std::string a_removed = remove_entry(1, "a", 0, 5);
    const std::string b = normal_entry(1, "b", "v", 1, 0);

    sorted_file_writer writer(file());
    writer.write(view_of(a_new));
    writer.write(view_of(a_old));
    writer.write(view_of(a_removed));
    writer.write(view_of(b));
    EXPECT_EQ(writer.written_count(), 2U);
    writer.close();

    EXPECT_EQ(read_file(file()), a_new + b);
}

// closing without writing any entry leaves an empty file
TEST_F(sorted_file_writer_test, empty_file) {
    sorted_file_writer writer(file());
    writer.close();
    EXPECT_EQ(writer.written_count(), 0U);
    EXPECT_TRUE(boost::filesystem::exists(file()));
    EXPECT_EQ(boost::filesystem::file_size(file()), 0U);
}

// an existing file is truncated before writing
TEST_F(sorted_file_writer_test, truncates_existing_file) {
    {
        std::ofstream out(file().string(), std::ios::binary);
        out << std::string(100, 'x');
    }
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    sorted_file_writer writer(file());
    writer.write(view_of(e));
    writer.close();
    EXPECT_EQ(read_file(file()), e);
}

// writing a sorted sort_buffer in the order of entry_at() gives a file that satisfies the order and the removal of
// duplicates, and the file can be read back with the reader
TEST_F(sorted_file_writer_test, writes_sorted_buffer_and_reads_back) {
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string b_new = normal_entry(1, "b", "new", 3, 0);
    const std::string b_old = remove_entry(1, "b", 2, 0);
    const std::string c = normal_with_blob(1, "c", "v", 1, 0, {9});

    sort_buffer buf(1024);
    for (const auto& e : {b_old, c, a, b_new}) {
        ASSERT_EQ(buf.append(view_of(e)), sort_buffer::append_result::appended);
    }
    buf.sort();

    sorted_file_writer writer(file());
    for (std::size_t i = 0; i < buf.entry_count(); ++i) {
        writer.write(buf.entry_at(i));
    }
    writer.close();
    EXPECT_EQ(writer.written_count(), 3U);
    EXPECT_EQ(read_file(file()), a + b_new + c);

    sorted_file_reader reader(file());
    std::vector<std::string> entries;
    while (auto v = reader.next()) {
        entries.emplace_back(v->serialized_entry());
    }
    EXPECT_EQ(entries, (std::vector<std::string>{a, b_new, c}));
}

// written entries are accumulated in the stdio buffer and do not reach the file while they are smaller than the
// buffer; close() writes them all
TEST_F(sorted_file_writer_test, buffers_writes_until_close) {
    const std::string value(1000, 'v');
    std::string expected;
    sorted_file_writer writer(file());
    // about 64 KiB: larger than the default buffer (a few KiB), smaller than stream_buffer_size
    for (int i = 0; i < 64; ++i) {
        const std::string e = normal_entry(1, "k" + std::to_string(100 + i), value, 1, 0);
        writer.write(view_of(e));
        expected += e;
    }
    ASSERT_LT(expected.size(), sorted_file_writer::stream_buffer_size);
    EXPECT_EQ(boost::filesystem::file_size(file()), 0U);
    writer.close();
    EXPECT_EQ(read_file(file()), expected);
}

// destruction without close() closes the file, and what has been written remains
TEST_F(sorted_file_writer_test, destructor_closes_without_close) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    {
        sorted_file_writer writer(file());
        writer.write(view_of(e));
    }
    EXPECT_EQ(read_file(file()), e);
}

// the file cannot be created in a missing directory: limestone_io_exception with errno
TEST_F(sorted_file_writer_test, cannot_create_file) {
    const boost::filesystem::path missing = boost::filesystem::path(location) / "missing" / "sorted";
    try {
        sorted_file_writer writer(missing);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
        EXPECT_NE(std::string(e.what()).find("cannot create sorted file"), std::string::npos) << e.what();
    }
}

// a non-data entry, an order violation, write() after close() and a second close() are FATAL
TEST_F(sorted_file_writer_test, contract_violations_are_fatal) {
    const std::string b = normal_entry(1, "b", "v", 1, 0);
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    sorted_file_writer writer(file());
    EXPECT_DEATH(writer.write(view_of(marker(2, 1))), "not valid for entry type marker_begin");
    writer.write(view_of(b));
    EXPECT_DEATH(writer.write(view_of(a)), "entries are out of order");
    writer.close();
    EXPECT_DEATH(writer.write(view_of(b)), "sorted_file_writer:write already closed");
    EXPECT_DEATH(writer.close(), "sorted_file_writer:close already closed");
}

}  // namespace limestone::testing
