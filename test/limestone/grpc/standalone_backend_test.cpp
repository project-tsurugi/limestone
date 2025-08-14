/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include "limestone/grpc/backend/standalone_backend.h"
#include "wal_sync/wal_history.h"
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <vector>

using namespace limestone::grpc::backend;
using namespace limestone::internal;

namespace limestone::testing {

class standalone_backend_test : public ::testing::Test {
protected:
    const  boost::filesystem::path temp_dir = "/tmp/standalone_backend_test_dir";

    void SetUp() override {
        boost::filesystem::remove_all(temp_dir);
        boost::filesystem::create_directories(temp_dir);
    }
    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }
};

TEST_F(standalone_backend_test, list_wal_history_empty) {
    standalone_backend backend(temp_dir);
    auto result = backend.list_wal_history();
    EXPECT_TRUE(result.empty());
}

TEST_F(standalone_backend_test, list_wal_history_with_records) {
    wal_history wh(temp_dir);
    wh.append(100);
    wh.append(200);
    auto expected = wh.list();
    standalone_backend backend(temp_dir);
    auto actual = backend.list_wal_history();
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].epoch, actual[i].epoch);
        EXPECT_EQ(expected[i].unique_id, actual[i].unique_id);
        EXPECT_EQ(expected[i].timestamp, actual[i].timestamp);
    }
}

TEST_F(standalone_backend_test, get_log_dir_returns_constructor_value) {
    standalone_backend backend(temp_dir);
    EXPECT_EQ(backend.get_log_dir(), temp_dir);
}


} // namespace limestone::testing
