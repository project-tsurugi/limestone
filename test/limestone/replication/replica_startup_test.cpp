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
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "datastore_impl.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;

/**
 * Tests of the startup processing of the replica (datastore::start_as_replica()).
 */
class replica_startup_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/replica_startup_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    // Starts as a master
    void start_as_master() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
    }

    // Starts as a replica (ready() is not called)
    void start_as_replica() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_replica_role();
        datastore_->start_as_replica();
    }

    void stop() {
        datastore_->shutdown();
        datastore_ = nullptr;
    }

    // Creates the persistent data that has an entry of epoch 1 (durable) and an entry of epoch 2 (not durable).
    // The durable epoch is 1
    void make_data_with_nondurable_snippet() {
        start_as_master();
        datastore_->switch_epoch(1);
        lc0_->begin_session();
        lc0_->add_entry(1, "k1", "v1", {1, 0});
        lc0_->end_session();
        datastore_->switch_epoch(2);
        lc0_->begin_session();
        lc0_->add_entry(1, "k2", "v2", {2, 0});
        lc0_->end_session();
        // The epoch is not advanced to 3, so epoch 2 does not become durable
        stop();
    }

    [[nodiscard]] static std::string read_file(const boost::filesystem::path& path) {
        std::ifstream stream(path.string(), std::ios::binary);
        return std::string{std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
    }

    [[nodiscard]] std::vector<std::tuple<std::string, std::string>> read_all_entries() {
        std::vector<std::tuple<std::string, std::string>> result{};
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<limestone::api::cursor> cursor = snapshot->get_cursor();
        while (cursor->next()) {
            std::string key{};
            std::string value{};
            cursor->key(key);
            cursor->value(value);
            result.emplace_back(key, value);
        }
        return result;
    }

    const boost::filesystem::path wal_file_{boost::filesystem::path(location) / "pwal_0000"};
    const boost::filesystem::path snapshot_dir_{boost::filesystem::path(location) / "data"};

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    limestone::api::log_channel* lc0_{};
};

// It can start with empty persistent data. The received epoch is 0
TEST_F(replica_startup_test, starts_with_empty_data) {
    start_as_replica();

    EXPECT_EQ(datastore_->get_impl()->get_received_epoch(), 0U);
    EXPECT_FALSE(boost::filesystem::exists(snapshot_dir_));
    stop();
}

// The scan at startup invalidates the snippets beyond the durable epoch. The received epoch becomes the durable
// epoch. The snapshot file is not created, and epoch_id_switched_ and epoch_id_informed_ stay 0
TEST_F(replica_startup_test, invalidates_snippets_beyond_durable_epoch) {
    make_data_with_nondurable_snippet();
    boost::filesystem::remove_all(snapshot_dir_);
    const std::string wal_before = read_file(wal_file_);

    start_as_replica();

    EXPECT_EQ(datastore_->get_impl()->get_received_epoch(), 1U);
    EXPECT_EQ(datastore_->epoch_id_switched(), 0U);
    EXPECT_EQ(datastore_->epoch_id_informed(), 0U);
    EXPECT_FALSE(boost::filesystem::exists(snapshot_dir_));
    const std::string wal_after = read_file(wal_file_);
    EXPECT_EQ(wal_after.size(), wal_before.size());
    EXPECT_NE(wal_after, wal_before);
    stop();

    // After the invalidation, another startup does not change the WAL files
    start_as_replica();
    EXPECT_EQ(datastore_->get_impl()->get_received_epoch(), 1U);
    EXPECT_EQ(read_file(wal_file_), wal_after);
    stop();

    // Only the durable entry remains
    start_as_master();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(read_all_entries(), expected);
}

// When every snippet is durable, the WAL files are not rewritten
TEST_F(replica_startup_test, keeps_durable_snippets) {
    start_as_master();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    stop();
    const std::string wal_before = read_file(wal_file_);

    start_as_replica();

    EXPECT_EQ(datastore_->get_impl()->get_received_epoch(), 1U);
    EXPECT_EQ(read_file(wal_file_), wal_before);
    stop();
}

}  // namespace limestone::testing
