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

TEST_F(inproc_backend_test, list_wal_history_empty) {
    gen_datastore();
    inproc_backend backend(*datastore_, log_dir);
    auto result = backend.list_wal_history();
    EXPECT_TRUE(result.empty());
}

TEST_F(inproc_backend_test, list_wal_history_with_records) {
    gen_datastore();
    wal_history wh(log_dir);
    wh.append(300);
    wh.append(400);
    auto expected = wh.list();
    inproc_backend backend(*datastore_, log_dir);
    auto actual = backend.list_wal_history();
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].epoch, actual[i].epoch);
        EXPECT_EQ(expected[i].unique_id, actual[i].unique_id);
        EXPECT_EQ(expected[i].timestamp, actual[i].timestamp);
    }
}

TEST_F(inproc_backend_test, get_log_dir_returns_constructor_value) {
    gen_datastore();
    inproc_backend backend(*datastore_, log_dir);
    EXPECT_EQ(backend.get_log_dir(), log_dir);
}

} // namespace limestone::testing
