/*
 * Copyright 2026-2026 Project Tsurugi.
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

#include <gtest/gtest.h>

#include <cerrno>
#include <fstream>

#include <boost/filesystem.hpp>

#include "file_operations.h"

namespace limestone::testing {

using limestone::internal::real_file_operations;

class file_operations_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::remove_all(dir_);
        boost::filesystem::create_directories(dir_);
    }

    void TearDown() override {
        boost::filesystem::remove_all(dir_);
    }

    const boost::filesystem::path dir_{"/tmp/file_operations_test"};
};

// an existing directory can be synchronized
TEST_F(file_operations_test, fsync_directory_succeeds) {
    real_file_operations ops;
    EXPECT_EQ(ops.fsync_directory(dir_), 0);
}

// a missing directory returns -1 and sets errno
TEST_F(file_operations_test, fsync_directory_missing) {
    real_file_operations ops;
    errno = 0;
    EXPECT_EQ(ops.fsync_directory(dir_ / "missing"), -1);
    EXPECT_EQ(errno, ENOENT);
}

// a path that is not a directory returns -1 and sets errno
TEST_F(file_operations_test, fsync_directory_not_a_directory) {
    const boost::filesystem::path file = dir_ / "file";
    std::ofstream(file.string()) << "data";
    real_file_operations ops;
    errno = 0;
    EXPECT_EQ(ops.fsync_directory(file), -1);
    EXPECT_EQ(errno, ENOTDIR);
}

}  // namespace limestone::testing
