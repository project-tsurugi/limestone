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
#include <map>
#include <unistd.h>
#include "internal.h"
#include "log_entry.h"
#include "test_root.h"
#include "limestone/api/epoch_id_type.h"
#include "replication/replica_connector.h" 

namespace limestone::testing {

constexpr const char* location = "/tmp/log_channel_replication_test";

class log_channel_replication_test : public ::testing::Test {
public:
    virtual void SetUp() {
        if (system("rm -rf /tmp/log_channel_replication_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/log_channel_replication_test") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/log_channel_replication_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};


TEST_F(log_channel_replication_test, replica_connector_setter_getter)
{
    limestone::api::log_channel &channel = datastore_->create_channel(boost::filesystem::path(location));

    EXPECT_EQ(channel.get_replica_connector_for_test(), nullptr);

    auto connector = std::make_unique<limestone::replication::replica_connector>();
    channel.set_replica_connector(std::move(connector));

    EXPECT_NE(channel.get_replica_connector_for_test(), nullptr);
}

TEST_F(log_channel_replication_test, replica_connector_disable)
{
    limestone::api::log_channel &channel = datastore_->create_channel(boost::filesystem::path(location));

    auto connector = std::make_unique<limestone::replication::replica_connector>();
    channel.set_replica_connector(std::move(connector));
    EXPECT_NE(channel.get_replica_connector_for_test(), nullptr);

    channel.disable_replica_connector();
    EXPECT_EQ(channel.get_replica_connector_for_test(), nullptr);
}

}  // namespace limestone::testing
