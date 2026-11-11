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
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::locator_writer;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u64le;

namespace {

/**
 * @brief Reads the whole content of the file.
 * @param p the file
 * @return the content
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/**
 * @brief Makes the expected bytes of a locator file.
 * @param positions the values of the records
 * @param locatee_size the size of the locatee file written to the footer
 * @return the bytes
 */
std::string expected_bytes(const std::vector<std::uint64_t>& positions, std::uint64_t locatee_size) {
    std::string s = std::string("LIMELOC\0", 8) + u64le(lf::format_version);
    s.resize(lf::header_size, '\0');
    for (const std::uint64_t p : positions) {
        s += u64le(p);
    }
    s += u64le(locatee_size) + std::string("LIMELOC\x80", 8);
    return s;
}

/**
 * @brief Writes the bytes to the file.
 * @param p the file
 * @param bytes the content
 */
void write_file(const boost::filesystem::path& p, const std::string& bytes) {
    std::ofstream out(p.string(), std::ios::binary);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

class locator_writer_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/locator_writer_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path file() {
        return boost::filesystem::path(location) / "output.loc";
    }
    static boost::filesystem::path locatee() {
        return boost::filesystem::path(location) / "output";
    }
};

// The header and the footer are written even without records. The header is the magic, the format version, and
// the reserved area of 0
TEST_F(locator_writer_test, zero_records_have_header_and_footer) {
    locator_writer writer(file(), 100, true);
    writer.close(0);
    EXPECT_EQ(writer.record_count(), 0U);
    const std::string bytes = read_file(file());
    EXPECT_EQ(bytes.size(), lf::header_size + lf::footer_size);
    EXPECT_EQ(bytes, expected_bytes({}, 0));
}

// Records a position that reaches the threshold and advances the threshold to the next multiple. A position
// exactly at a multiple is also recorded
TEST_F(locator_writer_test, records_follow_the_interval) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);    // threshold 0 -> recorded, threshold 100
    writer.note_entry(50);
    writer.note_entry(99);
    writer.note_entry(100);  // recorded, threshold 200
    writer.note_entry(150);
    writer.note_entry(250);  // recorded (the first beyond 200), threshold 300
    writer.note_entry(299);
    writer.note_entry(300);  // recorded, threshold 400
    writer.close(400);
    EXPECT_EQ(writer.record_count(), 4U);
    EXPECT_EQ(read_file(file()), expected_bytes({0, 100, 250, 300}, 400));
}

// The first position received becomes record 0 even if it is not 0
TEST_F(locator_writer_test, first_entry_is_record_0) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(9);    // recorded, threshold 100
    writer.note_entry(99);
    writer.note_entry(100);  // recorded
    writer.close(200);
    EXPECT_EQ(read_file(file()), expected_bytes({9, 100}, 200));
}

// An entry that crosses several multiples still yields one record. The threshold becomes the next multiple of
// the position that crossed
TEST_F(locator_writer_test, crossing_multiple_multiples_records_once) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);     // recorded, threshold 100
    writer.note_entry(1000);  // recorded (crosses the multiples from 100 to 1000 at once), threshold 1100
    writer.note_entry(1050);
    writer.note_entry(1100);  // recorded
    writer.close(1200);
    EXPECT_EQ(read_file(file()), expected_bytes({0, 1000, 1100}, 1200));
}

// Receiving the same position again is not treated as going back (one record)
TEST_F(locator_writer_test, same_offset_twice_is_allowed) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);
    writer.note_entry(0);
    writer.close(10);
    EXPECT_EQ(read_file(file()), expected_bytes({0}, 10));
}

// A position that goes back aborts. note_entry after close also aborts
TEST_F(locator_writer_test, aborts_on_misuse) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(10);
    EXPECT_DEATH(writer.note_entry(9), "went backward");
    writer.close(20);
    EXPECT_DEATH(writer.note_entry(20), "already closed");
}

// Passing close() a size of the locatee file less than the last position aborts
TEST_F(locator_writer_test, aborts_when_locatee_size_is_smaller_than_last_offset) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(50);
    EXPECT_DEATH(writer.close(49), "smaller than the last offset");
}

// The record interval of 0 aborts
TEST_F(locator_writer_test, aborts_on_zero_interval) {
    EXPECT_DEATH(locator_writer(file(), 0, true), "must not be 0");
}

// limestone_io_exception if the file is specified where it cannot be created
TEST_F(locator_writer_test, throws_when_file_cannot_be_created) {
    EXPECT_THROW(locator_writer(boost::filesystem::path(location) / "no_such_dir" / "x.loc", 100, true),
                 limestone_io_exception);
}

// build_from_file records the positions of the data entries of the locatee file by the same rule as while it is
// written. The marker and the storage entries are not recorded, but the position advances
TEST_F(locator_writer_test, build_from_file_matches_writing) {
    const std::string m = marker(2, 0);                              // 9 bytes, position 0
    const std::string e1 = normal_entry(1, "a", "v", 1, 1);          // a data entry
    const std::string s1 = storage_entry(8, 1, 1, 1);                // add_storage (not recorded)
    const std::string e2 = remove_entry(1, "b", 1, 2);               // a data entry
    const std::string e3 = normal_with_blob(1, "c", "v", 1, 3, {7}); // a data entry
    const std::string e4 = normal_entry(1, "d", "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv", 1, 4);
    const std::string contents = m + e1 + s1 + e2 + e3 + e4;
    write_file(locatee(), contents);

    const std::uint64_t interval = 40;
    const std::size_t count = locator_writer::build_from_file(locatee(), file(), interval);

    // Matches the result of passing the same positions to note_entry
    const boost::filesystem::path expected_file = boost::filesystem::path(location) / "expected.loc";
    locator_writer writer(expected_file, interval, true);
    std::uint64_t offset = m.size();
    writer.note_entry(offset);
    offset += e1.size() + s1.size();
    writer.note_entry(offset);
    offset += e2.size();
    writer.note_entry(offset);
    offset += e3.size();
    writer.note_entry(offset);
    offset += e4.size();
    writer.close(offset);
    EXPECT_EQ(offset, contents.size());
    EXPECT_EQ(count, writer.record_count());
    EXPECT_EQ(read_file(file()), read_file(expected_file));
    // The data entries begin at 9, 69, 99 and 146, and all are recorded with the interval of 40
    EXPECT_EQ(count, 4U);
    EXPECT_EQ(read_file(file()), expected_bytes({9, 69, 99, 146}, 220));
}

// A locatee file with only the marker has 0 records, and the size in the footer is the size of the file
TEST_F(locator_writer_test, build_from_file_of_marker_only) {
    write_file(locatee(), marker(2, 0));
    EXPECT_EQ(locator_writer::build_from_file(locatee(), file(), 100), 0U);
    EXPECT_EQ(read_file(file()), expected_bytes({}, 9));
}

// limestone_io_exception if the locatee file ends in the middle of an entry
TEST_F(locator_writer_test, build_from_file_throws_on_broken_locatee) {
    const std::string e = normal_entry(1, "a", "v", 1, 1);
    write_file(locatee(), marker(2, 0) + e.substr(0, 3));
    EXPECT_THROW(locator_writer::build_from_file(locatee(), file(), 100), limestone_io_exception);
}

// limestone_io_exception if the locatee file does not exist
TEST_F(locator_writer_test, build_from_file_throws_when_locatee_is_missing) {
    EXPECT_THROW(locator_writer::build_from_file(locatee(), file(), 100), limestone_io_exception);
}

}  // namespace limestone::testing
