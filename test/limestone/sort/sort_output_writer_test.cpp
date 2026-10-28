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
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "log_entry.h"
#include "sort/sort_output_writer.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::api::log_entry;
using limestone::internal::log_entry_view;
using limestone::internal::sort_output_writer;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;

namespace {

/// the entry type of marker_begin
constexpr std::uint8_t marker_begin_type = 2;

/// the format at startup (snapshot): write_version as it is, remove_entries written, no fsync
const sort_output_writer::format keep_all{log_entry::write_version_handling::keep, true, false};

/// the format of the compaction: the oldest write_version, remove_entries not written, fsync
const sort_output_writer::format compaction{log_entry::write_version_handling::oldest, false, true};

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

class sort_output_writer_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sort_output_writer_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path file() {
        return boost::filesystem::path(location) / "output";
    }
};

// the output begins with a marker_begin of epoch 0 even without entries
TEST_F(sort_output_writer_test, empty_output_has_marker_begin) {
    sort_output_writer writer(file(), "test file", keep_all);
    writer.close();
    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0));
}

// the format writing write_version as it is and keeping remove_entries writes the entries of the three types as they
// are. The BLOB IDs are kept
TEST_F(sort_output_writer_test, keeps_entries_as_they_are) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = normal_with_blob(1, "b", "v", 11, 2, {100, 200});
    const std::string e3 = remove_entry(1, "c", 12, 3);

    sort_output_writer writer(file(), "test file", keep_all);
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e1 + e2 + e3);
}

// the format not writing remove_entries excludes only the remove_entries
TEST_F(sort_output_writer_test, drops_remove_entry) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = remove_entry(1, "b", 11, 2);
    const std::string e3 = normal_with_blob(1, "c", "v", 12, 3, {100});

    sort_output_writer writer(file(), "test file", {log_entry::write_version_handling::keep, false, false});
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e1 + e3);
}

// the format of the compaction replaces the write_version with the oldest one (0, 0), excludes remove_entries, and
// closes with fsync. The values and the BLOB IDs are kept
TEST_F(sort_output_writer_test, compaction_format) {
    sort_output_writer writer(file(), "test file", compaction);
    writer.write(view_of(normal_entry(1, "a", "value", 10, 1)));
    writer.write(view_of(remove_entry(1, "b", 11, 2)));
    writer.write(view_of(normal_with_blob(1, "c", "v", 12, 3, {100, 200})));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + normal_entry(1, "a", "value", 0, 0)
                                     + normal_with_blob(1, "c", "v", 0, 0, {100, 200}));
}

// destruction without close() closes the file, and what has been written remains
TEST_F(sort_output_writer_test, destructor_closes_without_close) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    {
        sort_output_writer writer(file(), "test file", keep_all);
        writer.write(view_of(e));
    }
    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e);
}

// the file cannot be created in a missing directory: limestone_io_exception with errno and the label
TEST_F(sort_output_writer_test, cannot_create_file) {
    const boost::filesystem::path missing = boost::filesystem::path(location) / "missing" / "output";
    try {
        sort_output_writer writer(missing, "test file", keep_all);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
        EXPECT_NE(std::string(e.what()).find("cannot create test file"), std::string::npos) << e.what();
    }
}

// a non-data entry and write() after close() are FATAL
TEST_F(sort_output_writer_test, contract_violations_are_fatal) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    sort_output_writer writer(file(), "test file", keep_all);
    EXPECT_DEATH(writer.write(view_of(marker(marker_begin_type, 1))), "not a data entry: marker_begin");
    writer.write(view_of(e));
    writer.close();
    EXPECT_DEATH(writer.write(view_of(e)), "sort_output_writer:write already closed");
}

// a second close() does nothing, with or without fsync (what has been written remains)
TEST_F(sort_output_writer_test, second_close_does_nothing) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    {
        sort_output_writer writer(file(), "test file", keep_all);
        writer.write(view_of(e));
        writer.close();
        writer.close();
        EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e);
    }
    {
        sort_output_writer writer(file(), "test file", compaction);
        writer.write(view_of(e));
        writer.close();
        writer.close();
        EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + normal_entry(1, "a", "v", 0, 0));
    }
}

}  // namespace limestone::testing
