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

#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include <test_root.h>

namespace limestone::testing {

constexpr const char* data_location = "/tmp/tp_monitor_test/data_location";
constexpr const char* metadata_location = "/tmp/tp_monitor_test/metadata_location";
constexpr const char* parent_directory = "/tmp/tp_monitor_test";

class tp_monitor_test : public ::testing::Test {
protected:
    void SetUp() override {
        if (std::system("rm -rf /tmp/tp_monitor_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (std::system(
                    "mkdir -p /tmp/tp_monitor_test/data_location "
                    "/tmp/tp_monitor_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() override {
        datastore_ = nullptr;
        if (std::system("rm -rf /tmp/tp_monitor_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    void prepare_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(tp_monitor_test, register_transaction_tpm_id_stores_mapping) { // NOLINT
    prepare_datastore();

    std::string tx_id = "tx-1";
    datastore_->register_transaction_tpm_id(tx_id, 42);

    auto const& txid_to_tpmid = datastore_->txid_to_tpmid();
    ASSERT_EQ(txid_to_tpmid.size(), 1U);
    auto iter = txid_to_tpmid.find(tx_id);
    ASSERT_TRUE(iter != txid_to_tpmid.end());
    EXPECT_EQ(iter->second, 42U);
}

TEST_F(tp_monitor_test, begin_session_with_tx_id_registers_epoch_txid) { // NOLINT
    prepare_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    datastore_->switch_epoch(1);

    std::string tx_id = "tx-1";
    datastore_->register_transaction_tpm_id(tx_id, 7);
    channel.begin_session(std::optional<std::string_view>(tx_id));
    channel.end_session();

    auto const& epoch_to_txids = datastore_->epoch_to_txids();
    auto iter = epoch_to_txids.find(static_cast<limestone::api::epoch_id_type>(1));
    ASSERT_TRUE(iter != epoch_to_txids.end());
    ASSERT_EQ(iter->second.size(), 1U);
    EXPECT_EQ(iter->second.front(), tx_id);
}

TEST_F(tp_monitor_test, begin_session_without_tx_id_does_not_register) { // NOLINT
    prepare_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    datastore_->switch_epoch(1);

    channel.begin_session(std::nullopt);
    channel.end_session();

    auto const& epoch_to_txids = datastore_->epoch_to_txids();
    EXPECT_TRUE(epoch_to_txids.empty());
}

} // namespace limestone::testing
