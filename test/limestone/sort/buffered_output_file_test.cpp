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
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "sort/buffered_output_file.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::buffered_output_file;

namespace {

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

/**
 * @brief Writes a string to the stream.
 * @param file the destination
 * @param s the string to write
 */
void put(const buffered_output_file& file, const std::string& s) {
    ASSERT_EQ(std::fwrite(s.data(), 1, s.size(), file.stream()), s.size());
}

}  // namespace

class buffered_output_file_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/buffered_output_file_test";
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

// what is written is accumulated in the buffer until close(), and close() makes it reach the file
TEST_F(buffered_output_file_test, close_writes_buffered_contents) {
    buffered_output_file f(file(), "test file");
    EXPECT_TRUE(f.is_open());
    EXPECT_EQ(f.file(), file());
    put(f, "abc");
    EXPECT_EQ(boost::filesystem::file_size(file()), 0U);
    f.close();
    EXPECT_FALSE(f.is_open());
    EXPECT_EQ(read_file(file()), "abc");
}

// sync_and_close() also makes what is written reach the file and closes it
TEST_F(buffered_output_file_test, sync_and_close_writes_buffered_contents) {
    buffered_output_file f(file(), "test file");
    put(f, "abc");
    f.sync_and_close();
    EXPECT_FALSE(f.is_open());
    EXPECT_EQ(read_file(file()), "abc");
}

// destruction without close() closes the file, and what has been written remains
TEST_F(buffered_output_file_test, destructor_closes_without_close) {
    {
        buffered_output_file f(file(), "test file");
        put(f, "abc");
    }
    EXPECT_EQ(read_file(file()), "abc");
}

// an existing file is truncated before writing
TEST_F(buffered_output_file_test, truncates_existing_file) {
    {
        std::ofstream out(file().string(), std::ios::binary);
        out << std::string(100, 'x');
    }
    buffered_output_file f(file(), "test file");
    put(f, "abc");
    f.close();
    EXPECT_EQ(read_file(file()), "abc");
}

// the file cannot be created in a missing directory: limestone_io_exception with errno and the label
TEST_F(buffered_output_file_test, cannot_create_file) {
    const boost::filesystem::path missing = boost::filesystem::path(location) / "missing" / "output";
    try {
        buffered_output_file f(missing, "test file");
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
        EXPECT_NE(std::string(e.what()).find("cannot create test file"), std::string::npos) << e.what();
    }
}

// stream() after the file is closed is FATAL
TEST_F(buffered_output_file_test, stream_after_close_is_fatal) {
    buffered_output_file f(file(), "test file");
    f.close();
    EXPECT_DEATH((void) f.stream(), "buffered_output_file:stream already closed");
}

// close() after the file is closed does nothing (with or without fsync)
TEST_F(buffered_output_file_test, close_after_close_does_nothing) {
    {
        buffered_output_file f(file(), "test file");
        put(f, "abc");
        f.close();
        f.close();
        EXPECT_EQ(read_file(file()), "abc");
    }
    {
        buffered_output_file f(file(), "test file");
        put(f, "def");
        f.sync_and_close();
        f.close();
        EXPECT_EQ(read_file(file()), "def");
    }
}

// sync_and_close() after a successful sync_and_close() does nothing
TEST_F(buffered_output_file_test, sync_and_close_after_sync_and_close_does_nothing) {
    buffered_output_file f(file(), "test file");
    put(f, "abc");
    f.sync_and_close();
    f.sync_and_close();
    EXPECT_EQ(read_file(file()), "abc");
}

// sync_and_close() after the file is closed without fsync is FATAL, so as not to pretend that it is fsynced
TEST_F(buffered_output_file_test, sync_and_close_after_close_is_fatal) {
    buffered_output_file f(file(), "test file");
    f.close();
    EXPECT_DEATH(f.sync_and_close(), "buffered_output_file:sync_and_close already closed without fsync");
}

// a failure of the writing (fflush on /dev/full fails with ENOSPC) is thrown, and the file is closed anyway.
// sync_and_close() after a failed sync_and_close() is FATAL because the contents are not fsynced
TEST_F(buffered_output_file_test, flush_failure_closes_the_file) {
    if (!boost::filesystem::exists("/dev/full")) { GTEST_SKIP() << "no /dev/full"; }
    {
        buffered_output_file f("/dev/full", "test file");
        put(f, "abc");
        EXPECT_THROW(f.close(), limestone_io_exception);
        EXPECT_FALSE(f.is_open());
    }
    buffered_output_file f("/dev/full", "test file");
    put(f, "abc");
    try {
        f.sync_and_close();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOSPC);
        EXPECT_NE(std::string(e.what()).find("cannot flush test file"), std::string::npos) << e.what();
    }
    EXPECT_FALSE(f.is_open());
    EXPECT_DEATH(f.sync_and_close(), "buffered_output_file:sync_and_close already closed without fsync");
}

}  // namespace limestone::testing
