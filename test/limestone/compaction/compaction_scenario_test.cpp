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

#include "compaction_scenario_test_fixture.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

std::string_view to_string_view(compaction_mode mode) {
    switch (mode) {
        case compaction_mode::online: return "online"sv;
        case compaction_mode::offline: return "offline"sv;
    }
    return "unknown"sv;
}

std::ostream& operator<<(std::ostream& os, compaction_mode mode) {
    return os << to_string_view(mode);
}

void compaction_scenario_test::run_compaction(epoch_id_type epoch) {
    if (GetParam() == compaction_mode::online) {
        run_compact_with_epoch_switch(epoch);
        return;
    }
    // Offline compaction runs against a stopped datastore. Switch to the epoch first: online
    // compaction switches to it as part of its run, which makes the preceding epoch durable, and
    // compaction only ever covers durable entries. Without this the two modes would compact a
    // different set of entries, and the offline mode would drop the entries of the last epoch.
    datastore_->switch_epoch(epoch);
    datastore_->shutdown();
    datastore_ = nullptr;
    ASSERT_NO_FATAL_FAILURE(run_offline_compaction());
    gen_datastore();
    datastore_->switch_epoch(epoch);
}

INSTANTIATE_TEST_SUITE_P(, compaction_scenario_test,
                         ::testing::Values(compaction_mode::online, compaction_mode::offline),
                         [](::testing::TestParamInfo<compaction_mode> const& info) {
                             return std::string(to_string_view(info.param));
                         });

// A compaction of a log directory that holds no pwal file at all must leave the catalog
// untouched and must not produce a compacted file. Migrated from compaction_test.no_pwals so
// that both compaction modes are covered.
TEST_P(compaction_scenario_test, no_pwals) {
    gen_datastore();
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    datastore_->switch_epoch(1);
    ASSERT_NO_FATAL_FAILURE(run_compaction(2));

    // No pwal file is present, so there is nothing to compact: the catalog content must be
    // unchanged (the catalog file itself is still rewritten with the same values).
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
    pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    // A blob id high-water mark left by earlier allocations must survive a compaction that has
    // nothing to compact: lowering it would let blob ids be reused.
    catalog.update_catalog_file(0, 123, {}, {});
    catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_EQ(catalog.get_max_blob_id(), 123);

    ASSERT_NO_FATAL_FAILURE(run_compaction(3));

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 123);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
    pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());
}

// Verifies the behavior of remove_entry across a compaction: an entry removed after it was
// added disappears, an entry removed before it was re-added survives, and a removal of a key
// that was never added leaves nothing behind. Migrated from compaction_test.scenario03 so
// that both compaction modes are covered.
TEST_P(compaction_scenario_test, remove_entry_semantics) {

    // 1. Create multiple PWALs using two different storage IDs
    gen_datastore();
    datastore_->switch_epoch(1);

    // Storage ID 1: key1 added, then removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->remove_entry(1, "key1", {1, 1});  // use remove_entry
    lc0_->end_session();

    // Storage ID 2: key2 added, no removal
    lc1_->begin_session();
    lc1_->add_entry(2, "key2", "value2", {1, 0});
    lc1_->end_session();

    // Storage ID 1: key3 removed first, then added
    lc2_->begin_session();
    lc2_->remove_entry(1, "key3", {1, 0});
    lc2_->add_entry(1, "key3", "value3", {1, 3});
    lc2_->end_session();

    // Storage ID 1: key4 deleted without adding
    lc0_->begin_session();
    lc0_->remove_entry(1, "key4", {1, 0});
    lc0_->end_session();

    datastore_->switch_epoch(2);

    // Check the created PWAL files
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    auto log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key1", std::nullopt, 1, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key4", std::nullopt, 1, 0, {}, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 1);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key2", "value2", 1, 0, {}, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", std::nullopt, 1, 0, {}, log_entry ::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key3", "value3", 1, 3, {}, log_entry::entry_type::normal_entry));

    // 2. Execute compaction
    ASSERT_NO_FATAL_FAILURE(run_compaction(3));

    // Check the catalog after compaction. The compacted file is registered by both modes; only
    // online compaction keeps the source pwal files as detached pwals. Epoch 2 carries no data,
    // so it never reaches the epoch file: offline compaction records the durable epoch 1 it finds
    // there, while online compaction records the epoch it rotated at.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), records_rotation_epoch() ? 2 : 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), retains_detached_pwals() ? 3 : 0);

    pwals = extract_pwal_files_from_datastore();
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    // Online compaction leaves the three rotated pwal files next to the compacted file;
    // offline compaction rebuilds the directory and keeps the compacted file only.
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 4 : 1);

    // The compacted file itself must be identical in both modes: only the effective entries
    // survive, and their write version is reset.
    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0

    // 3. Add/Update PWALs (include remove_entry again)

    // Storage ID 1: key11 added, then removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key11", "value1", {2, 0});
    lc0_->remove_entry(1, "key11", {2, 1});  // use remove_entry
    lc0_->end_session();

    // Storage ID 2: key21 added, no removal
    lc1_->begin_session();
    lc1_->add_entry(2, "key21", "value2", {2, 0});
    lc1_->end_session();

    // Storage ID 1: key31 removed first, then added
    lc2_->begin_session();
    lc2_->remove_entry(1, "key31", {2, 0});
    lc2_->add_entry(1, "key31", "value3", {2, 3});
    lc2_->end_session();

    // Storage ID 1: key41 deleted without adding
    lc0_->begin_session();
    lc0_->remove_entry(1, "key41", {2, 0});
    lc0_->end_session();

    datastore_->switch_epoch(4);

    // Check the newly created PWAL files. Each channel writes a new pwal file next to the
    // compacted one; online compaction also left the rotated source files behind.
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 7 : 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 2, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key11", std::nullopt, 2, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 1);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key21", "value2", 2, 0, {}, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key31", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, {}, log_entry::entry_type::normal_entry));

    // 4. Restart the datastore and read the snapshot
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    // 5. Check the compacted file and the snapshot created at boot time
    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("data/snapshot", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", std::nullopt, 2, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key21", "value2", 2, 0, {}, log_entry::entry_type::normal_entry));

    // 6. Verify the snapshot contents after restart
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "key3");
    EXPECT_EQ(kv_list[0].second, "value3");
    EXPECT_EQ(kv_list[1].first, "key31");
    EXPECT_EQ(kv_list[1].second, "value3");
    EXPECT_EQ(kv_list[2].first, "key2");
    EXPECT_EQ(kv_list[2].second, "value2");
    EXPECT_EQ(kv_list[3].first, "key21");
    EXPECT_EQ(kv_list[3].second, "value2");
}

// Verifies the behavior of remove_storage across a compaction: entries written to a storage
// before it was removed disappear, and entries written to the same storage afterwards survive.
// Migrated from compaction_test.scenario04 so that both compaction modes are covered.
TEST_P(compaction_scenario_test, remove_storage_semantics) {

    gen_datastore();
    datastore_->switch_epoch(1);

    // Storage ID 1: Add normal entries
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->add_entry(1, "key2", "value2", {1, 1});
    lc0_->end_session();

    // Storage ID 2: Add normal entries
    lc1_->begin_session();
    lc1_->add_entry(2, "key3", "value3", {1, 0});
    lc1_->add_entry(2, "key4", "value4", {1, 1});
    lc1_->end_session();

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(1, "key5", "value5", {1, 2});
    lc2_->add_entry(1, "key6", "value6", {1, 3});
    lc2_->end_session();

    datastore_->switch_epoch(2);

    // Remove storage for Storage ID 2
    lc1_->begin_session();
    lc1_->remove_storage(2, {2, 0});
    lc1_->end_session();

    datastore_->switch_epoch(3);

    // Add an entry to Storage ID 1
    lc0_->begin_session();
    lc0_->add_entry(1, "key7", "value7", {3, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2, after the storage was removed
    lc1_->begin_session();
    lc1_->add_entry(2, "key8", "value8", {3, 0});
    lc1_->end_session();

    // Check PWALs before compaction
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);

    std::vector<log_entry> log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 1, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key7", "value7", 3, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key3", "value3", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key4", "value4", 1, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "", "", 2, 0, {}, log_entry::entry_type::remove_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key8", "value8", 3, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key5", "value5", 1, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key6", "value6", 1, 3, {}, log_entry::entry_type::normal_entry));

    // Compaction: the entries of storage 2 written before its removal must be dropped, and
    // "key8", written after the removal, must survive.
    ASSERT_NO_FATAL_FAILURE(run_compaction(4));

    pwals = extract_pwal_files_from_datastore();
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 4 : 1);
    if (retains_detached_pwals()) {
        // the rotated file of each channel, next to the compacted file
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
    }

    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, {}, log_entry::entry_type::normal_entry));

    // Write further entries and remove storage 1 this time.

    // Storage ID 1: Add normal entries
    lc0_->begin_session();
    lc0_->add_entry(1, "key11", "value1", {4, 0});
    lc0_->add_entry(1, "key12", "value2", {4, 1});
    lc0_->end_session();

    // Storage ID 2: Add normal entries
    lc1_->begin_session();
    lc1_->add_entry(2, "key13", "value3", {4, 0});
    lc1_->add_entry(2, "key14", "value4", {4, 1});
    lc1_->end_session();

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(1, "key15", "value5", {4, 2});
    lc2_->add_entry(1, "key16", "value6", {4, 3});
    lc2_->end_session();

    datastore_->switch_epoch(5);

    // Remove storage for Storage ID 1
    lc1_->begin_session();
    lc1_->remove_storage(1, {5, 0});
    lc1_->end_session();

    datastore_->switch_epoch(6);

    // Add an entry to Storage ID 1, after the storage was removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key17", "value7", {6, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2
    lc1_->begin_session();
    lc1_->add_entry(2, "key18", "value8", {6, 0});
    lc1_->end_session();

    datastore_->switch_epoch(7);

    // Check the newly created PWAL files
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 7 : 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    if (retains_detached_pwals()) {
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
    }

    log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key12", "value2", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key17", "value7", 6, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key13", "value3", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key14", "value4", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "", "", 5, 0, {}, log_entry::entry_type::remove_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key15", "value5", 4, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key16", "value6", 4, 3, {}, log_entry::entry_type::normal_entry));

    // Restart the datastore and read the snapshot
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    // The compacted file must be unchanged by the restart.
    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, {}, log_entry::entry_type::normal_entry));

    // The snapshot built at boot time keeps only the entries that survive the removal of
    // storage 1: "key17", written after the removal, and the entries of storage 2.
    log_entries = read_log_file("data/snapshot", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key17", "value7", 6, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key13", "value3", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "key14", "value4", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, {}, log_entry::entry_type::normal_entry));

    ASSERT_EQ(kv_list.size(), 5);
    EXPECT_EQ(kv_list[0].first, "key17");
    EXPECT_EQ(kv_list[0].second, "value7");
    EXPECT_EQ(kv_list[1].first, "key13");
    EXPECT_EQ(kv_list[1].second, "value3");
    EXPECT_EQ(kv_list[2].first, "key14");
    EXPECT_EQ(kv_list[2].second, "value4");
    EXPECT_EQ(kv_list[3].first, "key18");
    EXPECT_EQ(kv_list[3].second, "value8");
    EXPECT_EQ(kv_list[4].first, "key8");
    EXPECT_EQ(kv_list[4].second, "value8");
}

// Verifies the handling of blob data across a compaction: the compacted file keeps the blob
// references of the surviving entries, the catalog tracks the blob id high-water mark, and the
// datastore hands out blob ids above it after a restart. Migrated from
// compaction_test.scenario_blob so that both compaction modes are covered.
TEST_P(compaction_scenario_test, blob_semantics) {

    gen_datastore();
    datastore_->switch_epoch(1);

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_blob_id(), 0);
    EXPECT_EQ(datastore_->next_blob_id(), 1);

    // Simulate a high-water mark left by earlier blob allocations. A compaction must never
    // lower it, otherwise blob ids would be reused.
    catalog.update_catalog_file(0, 123, {}, {});
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_blob_id(), 123);

    // Create two PWALs containing blobs.
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0}, {1001, 1002});
    lc0_->add_entry(1, "key2", "value2", {1, 1}, {1003, 1004});
    lc0_->end_session();

    lc1_->begin_session();
    lc1_->add_entry(2, "key3", "value3", {1, 0}, {1005, 1006});
    lc1_->end_session();

    datastore_->switch_epoch(2);

    // The blob files must exist on disk, so that a compaction that carries them over (or
    // collects them) can be observed.
    boost::filesystem::path path1001 = create_dummy_blob_files(1001);
    boost::filesystem::path path1002 = create_dummy_blob_files(1002);
    boost::filesystem::path path1003 = create_dummy_blob_files(1003);
    boost::filesystem::path path1004 = create_dummy_blob_files(1004);
    boost::filesystem::path path1005 = create_dummy_blob_files(1005);
    boost::filesystem::path path1006 = create_dummy_blob_files(1006);
    datastore_->set_next_blob_id(1007);

    std::vector<log_entry> log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 1, 1, {1003, 1004}, log_entry::entry_type::normal_with_blob));

    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 1);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key3", "value3", 1, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);

    ASSERT_NO_FATAL_FAILURE(run_compaction(3));

    // The catalog must track the highest blob id referenced by the surviving entries. Epoch 2
    // carries no data and so never reaches the epoch file, which is why the two modes record a
    // different epoch here (see records_rotation_epoch()); the later compactions below follow
    // epochs that do carry data, and both modes then record the same epoch.
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), records_rotation_epoch() ? 2 : 1);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    ASSERT_EQ(catalog.get_detached_pwals().size(), retains_detached_pwals() ? 2 : 0);
    if (retains_detached_pwals()) {
        // the detached pwals are the rotated source files, one per channel that wrote
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[0].find("pwal_0000.") == 0);
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[1].find("pwal_0001.") == 0);
    }

    pwals = extract_pwal_files_from_datastore();
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 3 : 1);
    if (retains_detached_pwals()) {
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    }

    // The compacted file must keep the blob ids of the surviving entries.
    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    // The blob files of the surviving entries must be there in both modes: they are still
    // referenced, so neither the blob garbage collector nor the rebuild of the log directory
    // may drop them.
    EXPECT_TRUE(boost::filesystem::exists(path1001));
    EXPECT_TRUE(boost::filesystem::exists(path1002));
    EXPECT_TRUE(boost::filesystem::exists(path1003));
    EXPECT_TRUE(boost::filesystem::exists(path1004));
    EXPECT_TRUE(boost::filesystem::exists(path1005));
    EXPECT_TRUE(boost::filesystem::exists(path1006));

    // Adding entries without blobs must not lower the high-water mark.
    lc2_->begin_session();
    lc2_->add_entry(1, "key15", "value5", {3, 0});
    lc2_->add_entry(1, "key16", "value6", {3, 1});
    lc2_->end_session();

    ASSERT_NO_FATAL_FAILURE(run_compaction(4));

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 3);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    ASSERT_EQ(catalog.get_detached_pwals().size(), retains_detached_pwals() ? 3 : 0);
    if (retains_detached_pwals()) {
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[2].find("pwal_0002.") == 0);
    }

    pwals = extract_pwal_files_from_datastore();
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 4 : 1);
    if (retains_detached_pwals()) {
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
    }

    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 5);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key15", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key16", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    // Adding an entry whose blob ids are all below the high-water mark must not change it.
    lc2_->begin_session();
    lc2_->add_entry(2, "key5", "value5", {4, 0}, {128, 32, 59});
    lc2_->end_session();
    create_dummy_blob_files(128);
    create_dummy_blob_files(32);
    create_dummy_blob_files(59);

    ASSERT_NO_FATAL_FAILURE(run_compaction(5));

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 4);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    ASSERT_EQ(catalog.get_detached_pwals().size(), retains_detached_pwals() ? 4 : 0);
    if (retains_detached_pwals()) {
        // lc2_ wrote and was rotated twice, so it contributes two detached pwals
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[0].find("pwal_0000.") == 0);
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[1].find("pwal_0001.") == 0);
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[2].find("pwal_0002.") == 0);
        EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[3].find("pwal_0002.") == 0);
    }

    pwals = extract_pwal_files_from_datastore();
    ASSERT_PRED_FORMAT2(ContainsString, pwals, compacted_filename);
    EXPECT_EQ(pwals.size(), retains_detached_pwals() ? 5 : 1);
    if (retains_detached_pwals()) {
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
        ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 2);
    }

    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key15", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key16", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key5", "value5", 0, 0, {128, 32, 59}, log_entry::entry_type::normal_with_blob));

    // After a restart the datastore must hand out blob ids above the high-water mark, so that a
    // blob id is never reused.
    datastore_->shutdown();
    datastore_ = nullptr;
    gen_datastore();
    EXPECT_EQ(datastore_->next_blob_id(), 1007);
}

// A blob file that is no longer referenced by any live entry is eventually removed in both
// modes, and a blob file that is still referenced survives in both modes. The two modes collect
// the unreferenced files at a different point: online compaction runs the blob garbage collector
// itself, whereas the tglogutil command carries the whole blob directory over untouched and the
// collection only happens when the datastore is started again on the compacted directory.
TEST_P(compaction_scenario_test, unreferenced_blob_files) {
    gen_datastore();
    prepare_blob_gc_test_data();

    EXPECT_TRUE(boost::filesystem::exists(path1001_));
    EXPECT_TRUE(boost::filesystem::exists(path1002_));
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));

    ASSERT_NO_FATAL_FAILURE(run_compaction(5));

    // blob_key1 was updated in epoch 4, so the blobs 1001 and 1002 of its epoch 3 version are no
    // longer referenced and must be gone once the compaction has been applied.
    datastore_->wait_for_blob_file_garbace_collector();
    EXPECT_FALSE(boost::filesystem::exists(path1001_));
    EXPECT_FALSE(boost::filesystem::exists(path1002_));

    // These blobs are still referenced by a live entry and must survive in both modes.
    EXPECT_TRUE(boost::filesystem::exists(path1003_));
    EXPECT_TRUE(boost::filesystem::exists(path2001_));
    EXPECT_TRUE(boost::filesystem::exists(path2002_));

    // The surviving entries and their blob references must be identical in both modes.
    std::vector<log_entry> log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob_key1", "blob_value1_epoch2", 0, 0, {2001, 2002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob_key2", "blob_value2", 0, 0, {1003}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "noblob_key1", "noblob_value1_epoch2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "noblob_key2", "noblob_value2", 0, 0, {}, log_entry::entry_type::normal_entry));
}

// A compaction of a log directory that already holds a compacted file must read that file as an
// input: the entries it holds have to survive, updates and removals written since then have to be
// applied to them, and the blob ids they reference must keep the high-water mark up. This covers
// running the compaction repeatedly on the same log directory, which is what happens in practice.
TEST_P(compaction_scenario_test, compaction_of_already_compacted_directory) {
    gen_datastore();
    datastore_->switch_epoch(1);

    // Entries the first compaction will fold into the compacted file: "keep" is never touched
    // again, "update" and "remove" are modified after the first compaction.
    lc0_->begin_session();
    lc0_->add_entry(1, "keep", "keep_v1", {1, 0});
    lc0_->add_entry(1, "update", "update_v1", {1, 1});
    lc0_->add_entry(1, "remove", "remove_v1", {1, 2});
    lc0_->end_session();

    // An entry with a blob, so that the blob id high-water mark has to survive the second
    // compaction as well.
    lc1_->begin_session();
    lc1_->add_entry(1, "blob", "blob_v1", {1, 3}, {5001});
    lc1_->end_session();

    datastore_->switch_epoch(2);
    boost::filesystem::path blob_path = create_dummy_blob_files(5001);
    datastore_->set_next_blob_id(5002);

    ASSERT_NO_FATAL_FAILURE(run_compaction(3));

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_EQ(catalog.get_max_blob_id(), 5001);

    std::vector<log_entry> log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "blob", "blob_v1", 0, 0, {5001}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "keep", "keep_v1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "remove", "remove_v1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "update", "update_v1", 0, 0, {}, log_entry::entry_type::normal_entry));

    // Update one entry and remove another, then compact again. The compacted file produced by the
    // first run is now an input of the second one.
    lc0_->begin_session();
    lc0_->add_entry(1, "update", "update_v2", {3, 0});
    lc0_->remove_entry(1, "remove", {3, 1});
    lc0_->add_entry(1, "added", "added_v1", {3, 2});
    lc0_->end_session();

    datastore_->switch_epoch(4);

    ASSERT_NO_FATAL_FAILURE(run_compaction(5));

    // The second compaction must still register exactly one compacted file, and the blob id
    // high-water mark must not be lowered even though the blob is only referenced by an entry
    // that came from the previous compacted file.
    catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_max_blob_id(), 5001);

    // The entry that was never touched again survives, the updated one carries its new value, the
    // removed one is gone, and the entry added after the first compaction is there.
    log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "added", "added_v1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "blob", "blob_v1", 0, 0, {5001}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "keep", "keep_v1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "update", "update_v2", 0, 0, {}, log_entry::entry_type::normal_entry));

    // The blob is still referenced, so it must not have been collected by either mode.
    EXPECT_TRUE(boost::filesystem::exists(blob_path));

    // The snapshot rebuilt at startup must agree with the compacted file.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    std::map<std::string, std::string> kv;
    for (auto const& [key, value] : kv_list) {
        kv.emplace(key, value);
    }
    ASSERT_EQ(kv.size(), 4);
    EXPECT_EQ(kv.count("remove"), 0U);
    EXPECT_EQ(kv["keep"], "keep_v1");
    EXPECT_EQ(kv["update"], "update_v2");
    EXPECT_EQ(kv["added"], "added_v1");
    EXPECT_EQ(kv["blob"], "blob_v1");
    EXPECT_TRUE(boost::filesystem::exists(blob_path));
}

// Compacting a log directory whose only transaction log is the compacted file itself, with no
// pwal written since the previous compaction, must keep the data. The compacted file is an input
// of the compaction, so a "nothing to compact" shortcut that also skipped a directory holding
// only a compacted file would drop it: offline compaction rebuilds the log directory from
// scratch, so a compacted file that is not produced again is simply gone.
TEST_P(compaction_scenario_test, compaction_without_new_pwals_keeps_compacted_file) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->add_entry(1, "key2", "value2", {1, 1}, {6001});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    boost::filesystem::path blob_path = create_dummy_blob_files(6001);
    datastore_->set_next_blob_id(6002);

    ASSERT_NO_FATAL_FAILURE(run_compaction(3));

    // Drop the detached pwals that online compaction leaves behind, so that in both modes the
    // compacted file is the only transaction log left in the directory.
    for (boost::filesystem::directory_iterator it{boost::filesystem::path(location)}, end;
         it != end; ++it) {
        std::string name = it->path().filename().string();
        if (starts_with(name, "pwal_") && name != compacted_filename) {
            boost::filesystem::remove(it->path());
        }
    }
    {
        // Drop the detached pwals from the catalog as well, so that it stays consistent with the
        // files actually present. Load the catalog from its file: a default-constructed one would
        // reset the epoch and blob id high-water marks.
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        catalog.update_catalog_file(catalog.get_max_epoch_id(), catalog.get_max_blob_id(),
                                    {compacted_file_info{compacted_filename, 1}}, {});
    }
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / compacted_filename));

    // Compact again without having written a single new entry.
    ASSERT_NO_FATAL_FAILURE(run_compaction(4));

    // The compacted file must still be there, still registered, and still hold the data.
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / compacted_filename));
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_max_blob_id(), 6001);

    std::vector<log_entry> log_entries = read_log_file(compacted_filename, location);
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {6001}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(boost::filesystem::exists(blob_path));

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    std::map<std::string, std::string> kv;
    for (auto const& [key, value] : kv_list) {
        kv.emplace(key, value);
    }
    ASSERT_EQ(kv.size(), 2);
    EXPECT_EQ(kv["key1"], "value1");
    EXPECT_EQ(kv["key2"], "value2");
}

}  // namespace limestone::testing
