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


#include "limestone/grpc/backend/inproc_backend.h"
#include "wal_sync/wal_history.h"
#include "limestone/api/datastore.h"
#include "limestone/api/configuration.h"
#include "test_root.h"
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <vector>

using namespace limestone::grpc::backend;
using namespace limestone::internal;

namespace limestone::testing {

class inproc_backend_test : public ::testing::Test {
protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    boost::filesystem::path log_dir = "/tmp/inproc_backend_test";

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(log_dir);
        boost::filesystem::path metadata_location{log_dir};
        limestone::api::configuration conf(data_locations, metadata_location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void SetUp() override {
        boost::filesystem::remove_all(log_dir);
        boost::filesystem::create_directories(log_dir);
    }
    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(log_dir);
    }
};


TEST_F(inproc_backend_test, get_wal_history_response_empty) {
    gen_datastore();
    inproc_backend backend(*datastore_, log_dir);
    limestone::grpc::proto::WalHistoryResponse response;
    auto status = backend.get_wal_history_response(&response);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(response.records_size(), 0);
}


TEST_F(inproc_backend_test, get_wal_history_response_with_records) {
    gen_datastore();
    wal_history wh(log_dir);
    wh.append(300);
    wh.append(400);
    datastore_->ready();
    datastore_->switch_epoch(401);
    auto expected = wh.list();
    inproc_backend backend(*datastore_, log_dir);
    limestone::grpc::proto::WalHistoryResponse response;
    auto status = backend.get_wal_history_response(&response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(expected.size(), response.records_size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(response.last_epoch(), 400);
}

TEST_F(inproc_backend_test, get_log_dir_returns_constructor_value) {
    gen_datastore();
    inproc_backend backend(*datastore_, log_dir);
    EXPECT_EQ(backend.get_log_dir(), log_dir);
}

} // namespace limestone::testing
