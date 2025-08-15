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
#include "limestone/log_entry.h"
#include <boost/filesystem.hpp>
#include <vector>

using namespace limestone::grpc::backend;
using namespace limestone::internal;

namespace limestone::testing {

class standalone_backend_test : public ::testing::Test {
protected:
    const boost::filesystem::path temp_dir = "/tmp/standalone_backend_test_dir";

    void write_epoch_file(uint64_t epoch_id) {
        std::string epoch_file = temp_dir.string() + "/epoch";
        FILE* fp = std::fopen(epoch_file.c_str(), "wb");
        ASSERT_TRUE(fp != nullptr);
        limestone::api::log_entry::durable_epoch(fp, epoch_id);
        std::fclose(fp);
    }

    void SetUp() override {
        boost::filesystem::remove_all(temp_dir);
        boost::filesystem::create_directories(temp_dir);
    }
    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }
};


TEST_F(standalone_backend_test, get_wal_history_response_empty) {
    standalone_backend backend(temp_dir);
    auto response = backend.get_wal_history_response();
    EXPECT_EQ(response.records_size(), 0);
}


TEST_F(standalone_backend_test, get_wal_history_response_with_records) {
    wal_history wh(temp_dir);
    wh.append(100);
    wh.append(200);
    auto expected = wh.list();
    write_epoch_file(200);

    standalone_backend backend(temp_dir);
    auto response = backend.get_wal_history_response();
    ASSERT_EQ(expected.size(), response.records_size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), exp.timestamp);
    }
    EXPECT_EQ(response.last_epoch(), 200);
}

TEST_F(standalone_backend_test, get_log_dir_returns_constructor_value) {
    standalone_backend backend(temp_dir);
    EXPECT_EQ(backend.get_log_dir(), temp_dir);
}


} // namespace limestone::testing
