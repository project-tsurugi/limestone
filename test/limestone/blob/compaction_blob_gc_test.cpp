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

 #include "limestone/compaction/compaction_test_fixture.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;
  

TEST_F(compaction_test, basic_blob_gc_test) {
    // Epoch 1: Prepare initial entries.
    gen_datastore();
    datastore_->switch_epoch(1);

    // Create two entries with blob data using lc0.
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key1", "blob_value1", {1, 0}, {1001, 1002});
    lc0_->add_entry(1, "blob_key2", "blob_value2", {1, 1}, {1003});
    lc0_->end_session();

    // Create two entries without blob data using lc0.
    lc0_->begin_session();
    lc0_->add_entry(1, "noblob_key1", "noblob_value1", {1, 2});
    lc0_->add_entry(1, "noblob_key2", "noblob_value2", {1, 3});
    lc0_->end_session();

    // Epoch 2: Switch epoch and update some entries with the same keys.
    datastore_->switch_epoch(2);
    lc0_->begin_session();
    // Update "blob_key1" with new blob data.
    lc0_->add_entry(1, "blob_key1", "blob_value1_epoch2", {2, 0}, {2001, 2002});
    // Update "noblob_key1" with a new value.
    lc0_->add_entry(1, "noblob_key1", "noblob_value1_epoch2", {2, 1});
    lc0_->end_session();

    // Create dummy blob files for the blob IDs.
    auto path1001 = create_dummy_blob_files(1001);
    auto path1002 = create_dummy_blob_files(1002);
    auto path1003 = create_dummy_blob_files(1003);
    auto path2001 = create_dummy_blob_files(2001);
    auto path2002 = create_dummy_blob_files(2002);


    // Verify PWAL content before compaction.
    // Here, we assume that "pwal_0000" aggregates entries from both epoch 1 and epoch 2.
    std::vector<log_entry> log_entries = read_log_file("pwal_0000", location);
    // Expecting six entries: four from epoch 1 and two from epoch 2.
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob_key1", "blob_value1", 1, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob_key2", "blob_value2", 1, 1, {1003}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "noblob_key1", "noblob_value1", 1, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "noblob_key2", "noblob_value2", 1, 3, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "blob_key1", "blob_value1_epoch2", 2, 0, {2001, 2002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 1, "noblob_key1", "noblob_value1_epoch2", 2, 1, {}, log_entry::entry_type::normal_entry));

    EXPECT_TRUE(boost::filesystem::exists(path1001));
    EXPECT_TRUE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));

    // Perform compaction in epoch 3.
    run_compact_with_epoch_switch(3);

    // Verify compaction catalog.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    // Ensure that at least one compacted file exists.
    EXPECT_FALSE(catalog.get_compacted_files().empty());
    // Expect the max blob id to be updated to the highest blob id in use (i.e. 2002).
    EXPECT_EQ(catalog.get_max_blob_id(), 2002);

    // Verify the content of the compacted PWAL.
    // Assuming the compacted file is named "pwal_0000.compacted".
    log_entries = read_log_file("pwal_0000.compacted", location);
    // Expected effective state:
    // - "blob_key1": effective value from epoch 2 ("blob_value1_epoch2") with blob IDs {2001,2002}.
    // - "blob_key2": remains from epoch 1.
    // - "noblob_key1": updated in epoch 2.
    // - "noblob_key2": remains from epoch 1.
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob_key1", "blob_value1_epoch2", 0, 0, {2001, 2002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob_key2", "blob_value2", 0, 0, {1003}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "noblob_key1", "noblob_value1_epoch2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "noblob_key2", "noblob_value2", 0, 0, {}, log_entry::entry_type::normal_entry));

    // Verify the existence of the compacted blob files.
    // GC will not be performed because available_boundary_version remains at the initial value.
    EXPECT_TRUE(boost::filesystem::exists(path1001));
    EXPECT_TRUE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));
    

    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key5", "value5", {1, 1});
    lc0_->end_session();

    datastore_->switch_epoch(4);

    datastore_->switch_available_boundary_version({3,0});

    // Perform compaction in epoch 5.
    run_compact_with_epoch_switch(5);

    // Verify the existence of the compacted blob files.
    EXPECT_FALSE(boost::filesystem::exists(path1001));
    EXPECT_FALSE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));

    // Restart datastore and verify snapshot content.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "blob_key1");
    EXPECT_EQ(kv_list[0].second, "blob_value1_epoch2");
    EXPECT_EQ(kv_list[1].first, "blob_key2");
    EXPECT_EQ(kv_list[1].second, "blob_value2");
    EXPECT_EQ(kv_list[2].first, "noblob_key1");
    EXPECT_EQ(kv_list[2].second, "noblob_value1_epoch2");
    EXPECT_EQ(kv_list[3].first, "noblob_key2");
    EXPECT_EQ(kv_list[3].second, "noblob_value2");

    // Verify that no snapshot PWAL file exists.
    log_entries = read_log_file("data/snapshot", location);
    ASSERT_TRUE(log_entries.empty());

    // Verify that the blob files are still present.
    EXPECT_FALSE(boost::filesystem::exists(path1001));
    EXPECT_FALSE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));
}

TEST_F(compaction_test, basic_blob_gc_reboot_test) {
    // Epoch 1: Prepare initial entries.
    gen_datastore();
    datastore_->switch_epoch(1);

    // Create two entries with blob data using lc0.
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key1", "blob_value1", {1, 0}, {1001, 1002});
    lc0_->add_entry(1, "blob_key2", "blob_value2", {1, 1}, {1003});
    lc0_->end_session();

    // Create two entries without blob data using lc0.
    lc0_->begin_session();
    lc0_->add_entry(1, "noblob_key1", "noblob_value1", {1, 2});
    lc0_->add_entry(1, "noblob_key2", "noblob_value2", {1, 3});
    lc0_->end_session();

    // Epoch 2: Switch epoch and update some entries with the same keys.
    datastore_->switch_epoch(2);
    lc0_->begin_session();
    // Update "blob_key1" with new blob data.
    lc0_->add_entry(1, "blob_key1", "blob_value1_epoch2", {2, 0}, {2001, 2002});
    // Update "noblob_key1" with a new value.
    lc0_->add_entry(1, "noblob_key1", "noblob_value1_epoch2", {2, 1});
    lc0_->end_session();
    datastore_->switch_epoch(3);

    // Create dummy blob files for the blob IDs.
    auto path1001 = create_dummy_blob_files(1001);
    auto path1002 = create_dummy_blob_files(1002);
    auto path1003 = create_dummy_blob_files(1003);
    auto path2001 = create_dummy_blob_files(2001);
    auto path2002 = create_dummy_blob_files(2002);

    // Verify PWAL content before reboot.
    // Here, we assume that "pwal_0000" aggregates entries from both epoch 1 and epoch 2.
    std::vector<log_entry> log_entries = read_log_file("pwal_0000", location);
    // Expecting six entries: four from epoch 1 and two from epoch 2.
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob_key1", "blob_value1", 1, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob_key2", "blob_value2", 1, 1, {1003}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "noblob_key1", "noblob_value1", 1, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "noblob_key2", "noblob_value2", 1, 3, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "blob_key1", "blob_value1_epoch2", 2, 0, {2001, 2002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 1, "noblob_key1", "noblob_value1_epoch2", 2, 1, {}, log_entry::entry_type::normal_entry));

    EXPECT_TRUE(boost::filesystem::exists(path1001));
    EXPECT_TRUE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));

    // ----- Online compaction is NOT performed. -----

    // Instead, restart the datastore directly.
    FLAGS_v = 70;
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    FLAGS_v = 30;

    // Verify snapshot content.
    // Expected effective state:
    // - "blob_key1": updated in epoch 2 → "blob_value1_epoch2"
    // - "blob_key2": remains from epoch 1.
    // - "noblob_key1": updated in epoch 2 → "noblob_value1_epoch2"
    // - "noblob_key2": remains from epoch 1.
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "blob_key1");
    EXPECT_EQ(kv_list[0].second, "blob_value1_epoch2");
    EXPECT_EQ(kv_list[1].first, "blob_key2");
    EXPECT_EQ(kv_list[1].second, "blob_value2");
    EXPECT_EQ(kv_list[2].first, "noblob_key1");
    EXPECT_EQ(kv_list[2].second, "noblob_value1_epoch2");
    EXPECT_EQ(kv_list[3].first, "noblob_key2");
    EXPECT_EQ(kv_list[3].second, "noblob_value2");

    // Verify that no snapshot PWAL file exists.
    log_entries = read_log_file("data/snapshot", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob_key1", "blob_value1_epoch2", 2, 0, {2001, 2002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob_key2", "blob_value2", 1, 1, {1003}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "noblob_key1", "noblob_value1_epoch2", 2, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "noblob_key2", "noblob_value2", 1, 3, {}, log_entry::entry_type::normal_entry));

    // Verify that the blob files are still present.
    datastore_->wait_for_blob_file_garbace_collector();
    EXPECT_FALSE(boost::filesystem::exists(path1001));
    EXPECT_FALSE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));
}

}  // namespace limestone::testing
