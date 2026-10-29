/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <limestone/api/blob_file.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>

namespace limestone::testing {

using namespace limestone::api;

TEST(blob_file_test, constructor_with_default_availability) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path);

    EXPECT_EQ(blob.path(), test_path);
    EXPECT_FALSE(static_cast<bool>(blob));
}

TEST(blob_file_test, constructor_with_availability) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path, true);

    EXPECT_EQ(blob.path(), test_path);
    EXPECT_TRUE(static_cast<bool>(blob));
}

TEST(blob_file_test, path_returns_correct_value) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path);

    EXPECT_EQ(blob.path(), test_path);
}

} // namespace limestone::api
