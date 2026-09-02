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

// Suite name specific to this file, derived from the shared fixture
// (compaction_test). ctest selects tests on the premise that the suite name
// equals the file name, so using the shared fixture directly as the suite name
// would sweep this file's tests into another entry. That other entry
// (compaction_test) may run in parallel with this one, so pass a dedicated
// location instead of sharing the base fixture's default directory (as the
// fixture header requires).
class compaction_blob_gc_test : public compaction_test {
public:
    compaction_blob_gc_test() : compaction_test("/tmp/compaction_blob_gc_test") {}
};
  

// issue #144: blob GC at online compaction is disabled until the fundamental fix of
// #144, so this test, which verifies that the GC runs, is disabled. Re-enable it
// together with the fix.
TEST_F(compaction_blob_gc_test, DISABLED_basic_blob_gc_test) {
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
    datastore_->set_next_blob_id(2003);

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
    // Read the compacted file of the current generation as recorded by the catalog.
    {
        compaction_catalog catalog_after = compaction_catalog::from_catalog_file(location);
        log_entries = read_log_file(catalog_after.get_current_compacted_file_name().value(), location);
    }
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
    lc0_->add_entry(1, "noblob_key5", "noblob_value5", {1, 1});
    lc0_->end_session();

    datastore_->switch_epoch(4);

    datastore_->switch_available_boundary_version({3,0});

    // Perform compaction in epoch 5.
    FLAGS_v = 100;
    run_compact_with_epoch_switch(5);
    FLAGS_v = 30;

    // Verify the existence of the compacted blob files.
    EXPECT_FALSE(boost::filesystem::exists(path1001));
    EXPECT_FALSE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path2001));
    EXPECT_TRUE(boost::filesystem::exists(path2002));

    // Restart datastore and verify snapshot content.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 5);
    EXPECT_EQ(kv_list[0].first, "blob_key1");
    EXPECT_EQ(kv_list[0].second, "blob_value1_epoch2");
    EXPECT_EQ(kv_list[1].first, "blob_key2");
    EXPECT_EQ(kv_list[1].second, "blob_value2");
    EXPECT_EQ(kv_list[2].first, "noblob_key1");
    EXPECT_EQ(kv_list[2].second, "noblob_value1_epoch2");
    EXPECT_EQ(kv_list[3].first, "noblob_key2");
    EXPECT_EQ(kv_list[3].second, "noblob_value2");
    EXPECT_EQ(kv_list[4].first, "noblob_key5");
    EXPECT_EQ(kv_list[4].second, "noblob_value5");

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

TEST_F(compaction_blob_gc_test, basic_blob_gc_reboot_test) {
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
    // - "blob_key1": updated in epoch 2 -> "blob_value1_epoch2"
    // - "blob_key2": remains from epoch 1.
    // - "noblob_key1": updated in epoch 2 -> "noblob_value1_epoch2"
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

// Test that blob GC is executed when no backup is in progress.
// issue #144: blob GC at online compaction is disabled until the fundamental fix of
// #144, so this test, which verifies that the GC runs, is disabled. Re-enable it
// together with the fix.
TEST_F(compaction_blob_gc_test, DISABLED_blob_gc_executes_without_backup_test) {
    gen_datastore();
    prepare_blob_gc_test_data();
    FLAGS_v = 100;

    // Verify blob files exist before compaction.
    EXPECT_TRUE(boost::filesystem::exists(path1001_));
    EXPECT_TRUE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));
    
    // Perform compaction in epoch 5.
    run_compact_with_epoch_switch(5);
    
    // Verify that GC executed.
    EXPECT_FALSE(boost::filesystem::exists(path1001_));
    EXPECT_FALSE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));
}

// During a backup (the old API) the compaction itself returns early when it is
// requested, because the removal of the old generation would make the copy of a file
// already enumerated by the backup fail. The blob GC is therefore not run either.
TEST_F(compaction_blob_gc_test, blob_gc_skipped_during_old_backup_test) {
    gen_datastore();
    prepare_blob_gc_test_data();
    auto& backup = datastore_->begin_backup();  // old backup API

    // The early return means no rotation runs, so no help with the epoch switch is needed.
    datastore_->compact_with_online();

    // The compaction has not run (the catalog stays at the initial generation).
    compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
    EXPECT_EQ(catalog.get_generation(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());

    // The blob GC has not run either.
    EXPECT_TRUE(boost::filesystem::exists(path1001_));
    EXPECT_TRUE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));

    backup.notify_end_backup();
}

// During a backup (the new API) the compaction itself returns early when it is requested.
TEST_F(compaction_blob_gc_test, blob_gc_skipped_during_new_backup_test) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto backup = begin_backup_with_epoch_switch(backup_type::transaction, 2);  // new backup API

    prepare_blob_gc_test_data();

    // The early return means no rotation runs, so no help with the epoch switch is needed.
    datastore_->compact_with_online();

    // The compaction has not run (the catalog stays at the initial generation).
    compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
    EXPECT_EQ(catalog.get_generation(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());

    // The blob GC has not run either.
    EXPECT_TRUE(boost::filesystem::exists(path1001_));
    EXPECT_TRUE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));

    // backup->notify_end_backup();
}

// Test that blob GC is executed after an old backup has ended (using the backup API without arguments).
// issue #144: blob GC at online compaction is disabled until the fundamental fix of
// #144, so this test, which verifies that the GC runs, is disabled. Re-enable it
// together with the fix.
TEST_F(compaction_blob_gc_test, DISABLED_blob_gc_executes_after_old_backup_test) {
    gen_datastore();
    prepare_blob_gc_test_data();
    FLAGS_v = 100;
    auto& backup = datastore_->begin_backup();  // old backup API
    backup.notify_end_backup();
    
    run_compact_with_epoch_switch(5);
    
    // Verify that GC executed after the old backup ended.
    EXPECT_FALSE(boost::filesystem::exists(path1001_));
    EXPECT_FALSE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));
}

// Reproduction test for issue #144: a blob that is registered in a blob_pool but has
// not been passed to add_entry yet (one held by a transaction still in progress) must
// not be collected by the blob GC at online compaction. The GC exemption list is
// built only from the scan of the compaction inputs, so such a blob would be deleted
// if that GC ran (#144). The test passes now because that GC is disabled altogether,
// and it must keep passing after the fundamental fix of #144 re-enables the GC,
// pinning the behavior either way.
//
// The GC boundary (available_boundary_version) never exceeds the durable epoch by the
// discipline of the caller (shirakami); this test uses a boundary that respects it.
TEST_F(compaction_blob_gc_test, blob_registered_but_not_yet_logged_survives_gc) {
    gen_datastore();

    // Epoch 3: write one entry with blobs into the WAL (the control group that the
    // GC boundary protects)
    datastore_->switch_epoch(3);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key1", "blob_value1", {3, 0}, {1001, 1002});
    lc0_->end_session();
    datastore_->switch_epoch(4);

    auto path1001 = create_dummy_blob_files(1001);
    auto path1002 = create_dummy_blob_files(1002);
    datastore_->set_next_blob_id(2000);

    // Simulate a transaction in progress: register a blob through a blob_pool and do
    // not call add_entry (pre-commit state). Keep the pool alive without release.
    auto pool = datastore_->acquire_blob_pool();
    blob_id_type in_flight_blob = pool->register_data("in-flight blob data");
    boost::filesystem::path in_flight_path = datastore_->get_blob_file(in_flight_blob).path();
    ASSERT_TRUE(boost::filesystem::exists(in_flight_path));

    // Advance the GC boundary within the caller's discipline ({2,0}, below the durable
    // epoch 3). It exceeds the catalog max_epoch_id (initially 0), which is the
    // condition for the GC to run at online compaction.
    datastore_->switch_available_boundary_version({2, 0});

    // Compaction + blob GC (rotation boundary E = 4)
    run_compact_with_epoch_switch(5);

    // Control group: blobs recorded in the WAL have write_version {3,0} >= the GC
    // boundary {2,0}, so they are exempted unconditionally (high container).
    EXPECT_TRUE(boost::filesystem::exists(path1001));
    EXPECT_TRUE(boost::filesystem::exists(path1002));

    // The point: a blob that has not appeared in the WAL yet must not be deleted
    // (its entry is going to be written by add_entry with a write_version above the
    // GC boundary once the transaction commits).
    EXPECT_TRUE(boost::filesystem::exists(in_flight_path));
}

// Test that blob GC is executed after a new backup has ended (using the backup API with arguments).
// issue #144: blob GC at online compaction is disabled until the fundamental fix of
// #144, so this test, which verifies that the GC runs, is disabled. Re-enable it
// together with the fix.
TEST_F(compaction_blob_gc_test, DISABLED_blob_gc_executes_after_new_backup_test) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto backup = begin_backup_with_epoch_switch(backup_type::transaction, 2);  // new backup API

    prepare_blob_gc_test_data();
    
    backup->notify_end_backup();
    run_compact_with_epoch_switch(5);
    
    // Verify that GC executed after the new backup ended.
    EXPECT_FALSE(boost::filesystem::exists(path1001_));
    EXPECT_FALSE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));
}



}  // namespace limestone::testing
