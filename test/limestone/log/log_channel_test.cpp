/*
 * Copyright 2022-2023 tsurugi project.
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

#include <unistd.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* location = "/tmp/log_channel_test";

class log_channel_test : public ::testing::Test {
public:
    virtual void SetUp() {
        if (system("rm -rf /tmp/log_channel_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/log_channel_test") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<std::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        std::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/log_channel_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(log_channel_test, name) {
    limestone::api::log_channel& channel = datastore_->create_channel(std::filesystem::path(location));
    EXPECT_EQ(channel.file_path().string(), std::string(location) + "/pwal_0000");
}

TEST_F(log_channel_test, number_and_backup) {
    limestone::api::log_channel& channel1 = datastore_->create_channel(std::filesystem::path(location));
    limestone::api::log_channel& channel2 = datastore_->create_channel(std::filesystem::path(location));
    limestone::api::log_channel& channel3 = datastore_->create_channel(std::filesystem::path(location));
    limestone::api::log_channel& channel4 = datastore_->create_channel(std::filesystem::path(location));

    channel1.begin_session();
    channel2.begin_session();
    channel3.begin_session();
    channel4.begin_session();

    EXPECT_EQ(datastore_->log_channels().size(), 4);

    channel1.end_session();
    channel2.end_session();
    channel3.end_session();
    channel4.end_session();

    EXPECT_EQ(datastore_->log_channels().size(), 4);

    auto& backup = datastore_->begin_backup();
    auto files = backup.files();

    EXPECT_EQ(files.size(), 5);
    EXPECT_EQ(files.at(0).string(), std::string(location) + "/epoch");
    EXPECT_EQ(files.at(1).string(), std::string(location) + "/pwal_0000");
    EXPECT_EQ(files.at(2).string(), std::string(location) + "/pwal_0001");
    EXPECT_EQ(files.at(3).string(), std::string(location) + "/pwal_0002");
    EXPECT_EQ(files.at(4).string(), std::string(location) + "/pwal_0003");
}

TEST_F(log_channel_test, remove) {
    limestone::api::log_channel& channel = datastore_->create_channel(std::filesystem::path(location));

    channel.begin_session();
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.add_entry(42, "k2", "v2", {100, 4});
    channel.add_entry(42, "k3", "v3", {100, 4});
    channel.end_session();

    channel.begin_session();
    channel.remove_entry(42, "k2", {128, 0});
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    // expect: datastore has {k1:v1, k3:v3}, not required to be sorted
    std::map<std::string, std::string> m;
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        m[key] = value;
    }
    EXPECT_EQ(m.size(), 2);
    EXPECT_EQ(m["k1"], "v1");
    EXPECT_EQ(m["k3"], "v3");
}

}  // namespace limestone::testing
