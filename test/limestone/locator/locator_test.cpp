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
#include <cerrno>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "file_operations.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::locator;
using limestone::internal::file_operations;
using limestone::internal::locator_writer;
using limestone::internal::real_file_operations;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::u64le;

namespace {

/**
 * @brief Reads the whole content of the file.
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/**
 * @brief Writes the bytes to the file.
 */
void write_file(const boost::filesystem::path& p, const std::string& bytes) {
    std::ofstream out(p.string(), std::ios::binary);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

/**
 * @brief Verifies that the locator file is judged invalid: not an I/O exception, but a limestone_exception whose
 *        message contains expected is thrown
 * @param file the locator file
 * @param locatee_size the size of the locatee file given to locator
 * @param ops the file operations given to locator
 * @param expected the substring that the message must contain
 */
void expect_invalid(const boost::filesystem::path& file, std::uint64_t locatee_size, file_operations& ops,
                    const std::string& expected) {
    try {
        locator loc(file, locatee_size, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        FAIL() << "unexpected I/O exception: " << e.what();
    } catch (const limestone_exception& e) {
        EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
    }
}

/**
 * @brief Performs expect_invalid() with the real file operations.
 */
void expect_invalid(const boost::filesystem::path& file, std::uint64_t locatee_size, const std::string& expected) {
    real_file_operations ops{};
    expect_invalid(file, locatee_size, ops, expected);
}

}  // namespace

class locator_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/locator_test";
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
    /**
     * @brief Makes a valid locator file with locator_writer and returns its content.
     * @param positions the values of the records
     * @param locatee_size the size of the locatee file written to the footer
     * @return the bytes written
     */
    static std::string write_valid(const std::vector<std::uint64_t>& positions, std::uint64_t locatee_size) {
        locator_writer writer(file(), 1, true);
        for (const std::uint64_t p : positions) {
            writer.note_entry(p);
        }
        writer.close(locatee_size);
        return read_file(file());
    }
};

// What locator_writer wrote can be read back
TEST_F(locator_test, reads_what_writer_wrote) {
    write_valid({0, 100, 250}, 400);
    locator loc(file(), 400);
    EXPECT_EQ(loc.record_count(), 3U);
    EXPECT_EQ(loc.positions(), (std::vector<std::uint64_t>{0, 100, 250}));
    EXPECT_EQ(loc.position(0), 0U);
    EXPECT_EQ(loc.position(1), 100U);
    EXPECT_EQ(loc.position(2), 250U);
    EXPECT_EQ(loc.locatee_size(), 400U);
    EXPECT_EQ(loc.file(), file());
}

// A locator file with 0 records can also be read
TEST_F(locator_test, reads_zero_records) {
    write_valid({}, 9);
    locator loc(file(), 9);
    EXPECT_EQ(loc.record_count(), 0U);
    EXPECT_TRUE(loc.positions().empty());
    EXPECT_EQ(loc.locatee_size(), 9U);
}

// A record number out of range aborts
TEST_F(locator_test, position_out_of_range_aborts) {
    write_valid({0}, 9);
    locator loc(file(), 9);
    EXPECT_DEATH(static_cast<void>(loc.position(1)), "out of range");
}

// Invalid if the magic of the header does not match
TEST_F(locator_test, rejects_wrong_header_magic) {
    std::string bytes = write_valid({0}, 9);
    bytes[0] = 'X';
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the magic of the header");
}

// Invalid if the format version of the header is not 1
TEST_F(locator_test, rejects_wrong_format_version) {
    std::string bytes = write_valid({0}, 9);
    bytes.replace(8, 8, u64le(2));
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the format version 2 is not 1");
}

// Invalid if the size minus the header and the footer is not a multiple of 8
TEST_F(locator_test, rejects_size_not_multiple_of_record) {
    std::string bytes = write_valid({0, 100}, 200);
    bytes.insert(lf::header_size, "\0", 1);  // insert 1 byte into the records
    write_file(file(), bytes);
    expect_invalid(file(), 200, "is not header + records + footer");
}

// Invalid if shorter than the header and the footer
TEST_F(locator_test, rejects_too_short_file) {
    const std::string bytes = write_valid({}, 9);
    write_file(file(), bytes.substr(0, bytes.size() - 8));
    expect_invalid(file(), 9, "is not header + records + footer");
    write_file(file(), "");
    expect_invalid(file(), 9, "shorter than the header");
}

// Invalid if the magic of the footer does not match (the shape left by a stop during the write)
TEST_F(locator_test, rejects_wrong_footer_magic) {
    std::string bytes = write_valid({0}, 9);
    bytes.back() = '\0';
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the magic of the footer");
}

// Also invalid if the footer is missing entirely while the size condition holds (2 records look like the footer)
TEST_F(locator_test, rejects_missing_footer) {
    std::string bytes = write_valid({0, 100}, 200);
    bytes.resize(bytes.size() - lf::footer_size);
    write_file(file(), bytes);
    expect_invalid(file(), 200, "the magic of the footer");
}

// Invalid if the size of the locatee file in the footer does not match the given size
TEST_F(locator_test, rejects_locatee_size_mismatch) {
    write_valid({0}, 9);
    expect_invalid(file(), 10, "the locatee size 9 in the footer does not match the actual size 10");
    EXPECT_NO_THROW(locator(file(), 9));
}

// Invalid if the value of a record is at or below the previous record (reversed, or the same value)
TEST_F(locator_test, rejects_records_not_increasing) {
    std::string bytes = write_valid({0, 100, 250}, 400);
    // set record 1 (the 8 bytes after the first record following the header) to 300, reversed against record 2 (250)
    bytes.replace(lf::header_size + lf::record_size, lf::record_size, u64le(300));
    write_file(file(), bytes);
    expect_invalid(file(), 400, "the record 2 (250) is not after the record 1 (300)");
    // the same value is also invalid
    bytes.replace(lf::header_size + lf::record_size, lf::record_size, u64le(250));
    write_file(file(), bytes);
    expect_invalid(file(), 400, "the record 2 (250) is not after the record 1 (250)");
}

// Invalid if the value of a record is at or beyond the size of the locatee file
TEST_F(locator_test, rejects_record_beyond_locatee_size) {
    std::string bytes = write_valid({0, 100, 250}, 400);
    bytes.replace(lf::header_size + 2 * lf::record_size, lf::record_size, u64le(400));
    write_file(file(), bytes);
    expect_invalid(file(), 400, "the record 2 (400) is not within the locatee size 400");
}

// limestone_io_exception if the file does not exist
TEST_F(locator_test, throws_when_file_is_missing) {
    EXPECT_THROW(locator(file(), 0), limestone_io_exception);
}

// A directory can be opened but not read, so limestone_io_exception (an error of reading the header)
TEST_F(locator_test, throws_when_file_is_a_directory) {
    try {
        locator loc(location, 0);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EISDIR);
    }
}

// limestone_io_exception with errno if fstat fails
TEST_F(locator_test, throws_when_fstat_fails) {
    class : public real_file_operations {
    public:
        int fstat(int, struct stat*) override {
            errno = EIO;
            return -1;
        }
    } ops;
    write_valid({0}, 9);
    try {
        locator loc(file(), 9, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// limestone_io_exception with errno if the read after the header fails
TEST_F(locator_test, throws_when_reading_after_the_header_fails) {
    class : public real_file_operations {
    public:
        size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++calls_ == 1) {
                return real_file_operations::fread(ptr, size, count, stream);
            }
            errno = EIO;
            return 0;
        }
        int ferror(FILE*) override { return 1; }
    private:
        int calls_{};
    } ops;
    write_valid({0}, 9);
    try {
        locator loc(file(), 9, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// limestone_exception if the file shrinks after fstat (the read ends short without an error)
TEST_F(locator_test, rejects_file_shrunk_after_fstat) {
    class : public real_file_operations {
    public:
        size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++calls_ == 1) {
                return real_file_operations::fread(ptr, size, count, stream);
            }
            return 0;
        }
    private:
        int calls_{};
    } ops;
    write_valid({0}, 9);
    expect_invalid(file(), 9, ops, "shorter than its size");
}

}  // namespace limestone::testing
