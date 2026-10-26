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
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "sort/sorted_file_reader.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::sorted_file_reader;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u8;

class sorted_file_reader_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sorted_file_reader_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    /**
     * @brief Writes the bytes to a file and returns its path.
     * @param bytes the contents
     * @return the path
     */
    static boost::filesystem::path write_file(std::string_view bytes) {
        boost::filesystem::path p = boost::filesystem::path(location) / "sorted";
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }
    /**
     * @brief Checks that the message of the exception contains expected and that errno is 0.
     * @param e the exception
     * @param expected the string the message must contain
     */
    static void expect_broken(const limestone_io_exception& e, std::string_view expected) {
        EXPECT_EQ(e.error_code(), 0);
        EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
    }
};

// reads the entries in order and returns nullopt at the end, and after that
TEST_F(sorted_file_reader_test, reads_entries_then_end) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = normal_with_blob(1, "b", "v", 11, 2, {100, 200});
    const std::string e3 = remove_entry(1, "c", 12, 3);
    sorted_file_reader reader(write_file(e1 + e2 + e3));
    std::vector<std::string> entries;
    while (auto v = reader.next()) {
        entries.emplace_back(v->serialized_entry());
    }
    EXPECT_EQ(entries, (std::vector<std::string>{e1, e2, e3}));
    EXPECT_FALSE(reader.next().has_value());
}

// an empty file returns nullopt from the beginning
TEST_F(sorted_file_reader_test, empty_file) {
    sorted_file_reader reader(write_file(""));
    EXPECT_FALSE(reader.next().has_value());
}

// with a start position, a compacted WAL file is read skipping the marker_begin at its beginning
TEST_F(sorted_file_reader_test, start_offset_skips_marker_begin) {
    const std::string head = marker(2, 0);
    const std::string e1 = normal_entry(1, "a", "v", 0, 0);
    const std::string e2 = normal_entry(1, "b", "v", 0, 0);
    ASSERT_EQ(head.size(), 9U);
    sorted_file_reader reader(write_file(head + e1 + e2), 9);
    std::vector<std::string> entries;
    while (auto v = reader.next()) {
        entries.emplace_back(v->serialized_entry());
    }
    EXPECT_EQ(entries, (std::vector<std::string>{e1, e2}));
}

// a file ending in the middle of an entry returns the entries before it, then throws limestone_io_exception
// (without errno)
TEST_F(sorted_file_reader_test, truncated_entry_throws) {
    const std::string e1 = normal_entry(1, "a", "v", 1, 0);
    const std::string e2 = normal_entry(1, "b", "value", 1, 0);
    sorted_file_reader reader(write_file(e1 + e2.substr(0, e2.size() - 1)));
    ASSERT_TRUE(reader.next().has_value());
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        expect_broken(e, "sorted file is broken (unexpected EOF) at offset " + std::to_string(e1.size()));
    }
}

// an invalid entry type throws limestone_io_exception (without errno)
TEST_F(sorted_file_reader_test, invalid_type_throws) {
    const std::string e1 = normal_entry(1, "a", "v", 1, 0);
    sorted_file_reader reader(write_file(e1 + u8(0x7f)));
    ASSERT_TRUE(reader.next().has_value());
    try {
        (void) reader.next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        expect_broken(e, "sorted file is broken (unknown log_entry type 127) at offset " + std::to_string(e1.size()));
    }
}

// a marker or a storage entry in the file throws limestone_io_exception (without errno)
TEST_F(sorted_file_reader_test, non_data_entry_throws) {
    const std::string e1 = normal_entry(1, "a", "v", 1, 0);
    for (const auto& bad : {marker(3, 1), storage_entry(7, 1, 1, 0)}) {
        sorted_file_reader reader(write_file(e1 + bad + e1));
        ASSERT_TRUE(reader.next().has_value());
        try {
            (void) reader.next();
            FAIL() << "no exception";
        } catch (const limestone_io_exception& e) {
            expect_broken(e, "sorted file has a non-data entry (");
            expect_broken(e, ") at offset " + std::to_string(e1.size()));
        }
    }
}

// a missing file throws limestone_io_exception with errno; a start position beyond the size of the file throws it
// without errno
TEST_F(sorted_file_reader_test, open_failures_throw) {
    try {
        sorted_file_reader reader(boost::filesystem::path(location) / "missing");
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
    }
    const std::string e1 = normal_entry(1, "a", "v", 1, 0);
    try {
        sorted_file_reader reader(write_file(e1), e1.size() + 1);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), 0);
    }
}

}  // namespace limestone::testing
