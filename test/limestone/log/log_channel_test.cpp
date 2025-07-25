/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include "manifest.h"
#include "limestone/api/epoch_id_type.h"

#define LOGFORMAT_VER 2

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

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
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
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    EXPECT_EQ(channel.file_path().string(), std::string(location) + "/pwal_0000");
}

TEST_F(log_channel_test, number_and_backup) {
    limestone::api::log_channel& channel1 = datastore_->create_channel(boost::filesystem::path(location));
    limestone::api::log_channel& channel2 = datastore_->create_channel(boost::filesystem::path(location));
    limestone::api::log_channel& channel3 = datastore_->create_channel(boost::filesystem::path(location));
    limestone::api::log_channel& channel4 = datastore_->create_channel(boost::filesystem::path(location));

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

#if LOGFORMAT_VER == 1
    int manifest_file_num = 1;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;      
#else
    int manifest_file_num = 0;
#endif
    EXPECT_EQ(files.size(), 5 + manifest_file_num);
    int i = 0;
#if LOGFORMAT_VER >= 2
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/" + "compaction_catalog");
#endif
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/epoch");
#if LOGFORMAT_VER >= 1
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/" + std::string(limestone::internal::manifest::file_name));
#endif
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/pwal_0000");
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/pwal_0001");
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/pwal_0002");
    EXPECT_EQ(files.at(i++).string(), std::string(location) + "/pwal_0003");
}

static std::map<std::string, std::string> read_all_from_cursor(limestone::api::cursor* cursor) {
    std::map<std::string, std::string> m;
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        m[key] = value;
    }
    return m;
}

TEST_F(log_channel_test, remove) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

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
    auto m = read_all_from_cursor(cursor.get());
    EXPECT_EQ(m.size(), 2);
    EXPECT_EQ(m["k1"], "v1");
    EXPECT_EQ(m["k3"], "v3");
}

TEST_F(log_channel_test, skip_storage_add_remove) {
    // write log entry but not use at the moment...
    // (purpose of this test: check not to abort as unimplemented)
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

    channel.begin_session();
    channel.add_storage(42, {90, 4});
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.add_entry(42, "k2", "v2", {100, 4});
    channel.end_session();

    channel.begin_session();
    channel.remove_entry(42, "k1", {110, 0});
    channel.remove_entry(42, "k2", {110, 0});
    channel.end_session();

    channel.begin_session();
    channel.truncate_storage(42, {120, 4});
    channel.remove_storage(42, {120, 4});
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    auto m = read_all_from_cursor(cursor.get());
    EXPECT_EQ(m.size(), 0);
}

TEST_F(log_channel_test, remove_storage) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

    channel.begin_session();
    channel.add_storage(42, {90, 4});
    channel.add_entry(42, "42-100", "v1", {100, 4});
    channel.add_entry(43, "43-100", "v2", {100, 4});
    channel.end_session();

    channel.begin_session();
    channel.remove_storage(42, {110, 4});
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    auto m = read_all_from_cursor(cursor.get());
    EXPECT_EQ(m.size(), 1);
    EXPECT_EQ(m["43-100"], "v2");  // in another storage
}

TEST_F(log_channel_test, truncate_storage) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

    channel.begin_session();
    channel.add_storage(42, {90, 4});
    channel.add_entry(42, "42-100", "v1", {100, 4});
    channel.add_entry(43, "43-100", "v2", {100, 4});
    channel.end_session();

    channel.begin_session();
    channel.truncate_storage(42, {110, 4});
    channel.end_session();

    channel.begin_session();
    channel.add_entry(42, "42-120", "v3", {120, 4});
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    auto m = read_all_from_cursor(cursor.get());
    EXPECT_EQ(m.size(), 2);
    EXPECT_EQ(m["43-100"], "v2");  // in another storage
    EXPECT_EQ(m["42-120"], "v3");  // after truncate
}

TEST_F(log_channel_test, write_blob_entry) {
    FLAGS_v = 50;

    const limestone::api::epoch_id_type epoch_id = 31415;
    const limestone::api::storage_id_type storage_id = 12345;
    const std::string key = "this is a key";
    const std::string value = "this is a value";
    const limestone::api::write_version_type write_version = limestone::api::write_version_type(67898, 76543);
    const std::vector<limestone::api::blob_id_type> large_objects = {314, 1592, 65358};


    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

    channel.begin_session();
    channel.add_entry(storage_id, key, value, write_version, large_objects);
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    EXPECT_TRUE(cursor->next());
    EXPECT_EQ(cursor->storage(), storage_id);

    std::string buf_key;
    cursor->key(buf_key);
    EXPECT_EQ(buf_key, key);

    std::string buf_value;
    cursor->value(buf_value);
    EXPECT_EQ(buf_value, value);

    EXPECT_FALSE(cursor->next());
}

TEST_F(log_channel_test, write_blob_entry_empty_large_objects) {
    FLAGS_v = 50;

    const limestone::api::epoch_id_type epoch_id = 31415;
    const limestone::api::storage_id_type storage_id = 12345;
    const std::string key = "this is a key";
    const std::string value = "this is a value";
    const limestone::api::write_version_type write_version = limestone::api::write_version_type(67898, 76543);
    const std::vector<limestone::api::blob_id_type> large_objects = {};  // Empty large_objects

    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));

    channel.begin_session();
    channel.add_entry(storage_id, key, value, write_version, large_objects);
    channel.end_session();

    datastore_->ready();
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();

    EXPECT_TRUE(cursor->next());
    EXPECT_EQ(cursor->storage(), storage_id);

    std::string buf_key;
    cursor->key(buf_key);
    EXPECT_EQ(buf_key, key);

    std::string buf_value;
    cursor->value(buf_value);
    EXPECT_EQ(buf_value, value);

    EXPECT_FALSE(cursor->next());
}

}  // namespace limestone::testing
