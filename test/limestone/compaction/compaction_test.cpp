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

 #include "compaction_test_fixture.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;
    

TEST_F(compaction_test, no_pwals) {
    gen_datastore();
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    datastore_->switch_epoch(1);
    run_compact_with_epoch_switch(2);

    // No PWALs are present, so the catalog should not be updated.
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
    pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());
}

TEST_F(compaction_test, scenario01) {
    FLAGS_v = 50;

    gen_datastore();
    datastore_->switch_epoch(1);
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // First compaction.
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(get_location());
    // EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Compaction run without any changes to PWALs.
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Remove detached PWALs to ensure that only compacted files are read.
    [[maybe_unused]] int result = std::system(("rm " + std::string(get_location()) + "/pwal_000?.0*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    restart_datastore_and_read_snapshot();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    run_compact_with_epoch_switch(4);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    // Add a new PWALs.
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v11", {3, 4});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v12", {3, 4});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k3", "v13", {3, 4});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    run_compact_with_epoch_switch(5);
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Delete some detached PWALs.
    [[maybe_unused]] int result2 = std::system(("rm " + std::string(get_location()) + "/pwal_000[12].*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);

    // Some PWALs are newly created.
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v23", {5, 0});
    lc0_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);

    // Reboot without rotation.
    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);

    // Rotate without any data changes.
    run_compact_with_epoch_switch(6);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 2);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    // Some PWALs are newly created or updated.
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "k4", "v33", {6, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {6, 0});
    lc1_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // Rotate.
    run_compact_with_epoch_switch(8);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 7);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Some PWALs are newly created.

    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {8, 0});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k2", "v43", {8, 0});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // Rotate without reboot.
    run_compact_with_epoch_switch(9);
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 8);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 6);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v33");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v43");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");
    EXPECT_EQ(kv_list[3].first, "k4");
    EXPECT_EQ(kv_list[3].second, "v33");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
}

// This test case, scenario02, is a modified version of scenario01.
// In scenario02, all calls to `restart_datastore_and_read_snapshot()`
// and checks on the returned kv_list values have been removed. By
// removing these calls and checks, the test focuses on verifying the
// behavior of compaction and file management without restarting the
// datastore or checking key-value data consistency. Since the datastore
// is not restarted, the timing of when the set of PWAL files maintained
// by the datastore is updated differs from scenario01, and therefore the
// test expectations have been changed.
TEST_F(compaction_test, scenario02) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // First compaction.
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Compaction run without any changes to PWALs.
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Remove detached PWALs to ensure that only compacted files are read.
    [[maybe_unused]] int result = std::system(("rm " + std::string(get_location()) + "/pwal_000?.0*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    run_compact_with_epoch_switch(4);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    EXPECT_EQ(pwals.size(), 3);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Add new PWALs.
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v11", {3, 4});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v12", {3, 4});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k3", "v13", {3, 4});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 6);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    run_compact_with_epoch_switch(5);
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 4);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);  // Detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Delete some detached PWALs.
    [[maybe_unused]] int result2 = std::system(("rm " + std::string(get_location()) + "/pwal_000[12].*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Some PWALs are newly created.
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v23", {5, 0});
    lc0_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Rotate.
    run_compact_with_epoch_switch(6);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 5);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 2);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);  // Detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    // Some PWALs are newly created or updated.
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "k4", "v33", {6, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {6, 0});
    lc1_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // Rotate.
    run_compact_with_epoch_switch(8);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 7);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Some PWALs are newly created.
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {8, 0});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k2", "v43", {8, 0});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // Rotate.
    run_compact_with_epoch_switch(9);
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 8);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 6);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
}

// This test case verifies the correct behavior of `remove_entry`.
TEST_F(compaction_test, scenario03) {
    FLAGS_v = 50;  // set VLOG level to 50

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

    // Storeage ID 1: key4 deleted witout adding
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

    auto log_entries = read_log_file("pwal_0000", get_location());
    ASSERT_EQ(log_entries.size(), 3);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key1", std::nullopt, 1, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key4", std::nullopt, 1, 0, {}, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", get_location());
    ASSERT_EQ(log_entries.size(), 1);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key2", "value2", 1, 0, {}, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", get_location());
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", std::nullopt, 1, 0, {}, log_entry ::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key3", "value3", 1, 3, {}, log_entry::entry_type::normal_entry));

    // 2. Execute compaction
    run_compact_with_epoch_switch(3);

    // Check the catalog and PWALs after compaction
    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 2);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);  // Includes the compacted file
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 2);                                                                                 // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0

    // 3. Add/Update PWALs (include remove_entry again)

    // Storage ID 1: key1 added, then removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key11", "value1", {2, 0});
    lc0_->remove_entry(1, "key11", {2, 1});  // use remove_entry
    lc0_->end_session();

    // Storage ID 2: key2 added, no removal
    lc1_->begin_session();
    lc1_->add_entry(2, "key21", "value2", {2, 0});
    lc1_->end_session();

    // Storage ID 1: key3 removed first, then added
    lc2_->begin_session();
    lc2_->remove_entry(1, "key31", {2, 0});
    lc2_->add_entry(1, "key31", "value3", {2, 3});
    lc2_->end_session();

    // Storeage ID 1: key4 deleted witout adding
    lc0_->begin_session();
    lc0_->remove_entry(1, "key41", {2, 0});
    lc0_->end_session();

    datastore_->switch_epoch(4);
    pwals = extract_pwal_files_from_datastore();

    // Check the created PWAL files
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);  // 3 new pwals and 3 rotaed pwals and 1 compacted file
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    log_entries = read_log_file("pwal_0000", get_location());
    ASSERT_EQ(log_entries.size(), 3);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 2, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key11", std::nullopt, 2, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", get_location());
    ASSERT_EQ(log_entries.size(), 1);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key21", "value2", 2, 0, {}, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", get_location());
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key31", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, {}, log_entry::entry_type::normal_entry));

    // 4. Restart the datastore
    datastore_->shutdown();
    datastore_ = nullptr;
    gen_datastore();  // Restart

    // 5. check the compacted file and snapshot creating at the boot time
    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 2);                                                                                 // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));  // write version changed to 0

    log_entries = read_log_file("data/snapshot", get_location());
    ASSERT_EQ(log_entries.size(), 4);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", std::nullopt, 2, 1, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key21", "value2", 2, 0, {}, log_entry::entry_type::normal_entry));

    // 5. Verify the snapshot contents after restart
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    // key1 should exist with its updated value, key2 and key3 should be removed
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

// This test case verifies the correct behavior of `remove_storage`.
TEST_F(compaction_test, scenario04) {
    FLAGS_v = 50;  // set VLOG level to 50

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

    // Advance the epoch to 2
    datastore_->switch_epoch(2);

    // Remove storage for Storage ID 2
    lc1_->begin_session();
    lc1_->remove_storage(2, {2, 0});  // Removes the storage with ID 2
    lc1_->end_session();

    // Advance the epoch to 3
    datastore_->switch_epoch(3);

    // Add an entry to Storage ID 1
    lc0_->begin_session();
    lc0_->add_entry(1, "key7", "value7", {3, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2
    lc1_->begin_session();
    lc1_->add_entry(2, "key8", "value8", {3, 0});
    lc1_->end_session();

    // Chek PWALs before compaction
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);

    std::vector<log_entry> log_entries = read_log_file("pwal_0000", get_location());
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 1, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key7", "value7", 3, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0001", get_location());
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key3", "value3", 1, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key4", "value4", 1, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "", "", 2, 0, {}, log_entry::entry_type::remove_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key8", "value8", 3, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0002", get_location());
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key5", "value5", 1, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key6", "value6", 1, 3, {}, log_entry::entry_type::normal_entry));

    // online compaction
    run_compact_with_epoch_switch(4);

    // Check PWALs after compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, {}, log_entry::entry_type::normal_entry));

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

    // Advance the epoch to 5
    datastore_->switch_epoch(5);

    // Remove storage for Storage ID 1
    lc1_->begin_session();
    lc1_->remove_storage(1, {5, 0});  // Removes the storage with ID 2
    lc1_->end_session();

    // Advance the epoch to 6
    datastore_->switch_epoch(6);

    // Add an entry to Storage ID 1
    lc0_->begin_session();
    lc0_->add_entry(1, "key17", "value7", {6, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2
    lc1_->begin_session();
    lc1_->add_entry(2, "key18", "value8", {6, 0});
    lc1_->end_session();

    // Advance the epoch to 6
    datastore_->switch_epoch(7);

    // Chek newly created PWALs
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    log_entries = read_log_file("pwal_0000", get_location());
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key12", "value2", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key17", "value7", 6, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0001", get_location());
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key13", "value3", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key14", "value4", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "", "", 5, 0, {}, log_entry::entry_type::remove_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("pwal_0002", get_location());
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key15", "value5", 4, 2, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key16", "value6", 4, 3, {}, log_entry::entry_type::normal_entry));

    // Restart the datastore

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    // check the compacted file and snapshot creating at the boot time
    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, {}, log_entry::entry_type::normal_entry));

    log_entries = read_log_file("data/snapshot", get_location());
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key17", "value7", 6, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key13", "value3", 4, 0, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "key14", "value4", 4, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, {}, log_entry::entry_type::normal_entry));

    // 5. Verify the snapshot contents after restart

    // key1 should exist with its updated value, key2 and key3 should be removed
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

// This test case verifies the correct behavior of blob feature.
TEST_F(compaction_test, scenario_blob) {
    FLAGS_v = 50;  // set VLOG level to 50

    gen_datastore();
    datastore_->switch_epoch(1);

    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
    EXPECT_EQ(datastore_->next_blob_id(), 1);

    // No PWALs are present => The catalog should not be updated.
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);


    // Update the max_blob_id in the catalog
    catalog.update_catalog_file(0, 123, {}, {});
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 123);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    // No PWALs are present => The max_blob_id in the catalog should not be updated.
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_max_blob_id(), 123);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    // Create two PWALs containing BLOBs

    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0}, {1001, 1002});
    lc0_->add_entry(1, "key2", "value2", {1, 1}, {1003, 1004});
    lc0_->end_session();

    // Storage ID 2: Add normal entries
    lc1_->begin_session();
    lc1_->add_entry(2, "key3", "value3", {1, 0}, {1005, 1006});
    lc1_->end_session();

    datastore_->switch_epoch(4);

    // Check PWALs before compaction
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);

    std::vector<log_entry> log_entries = read_log_file("pwal_0000", get_location());
    ASSERT_EQ(log_entries.size(), 2);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 1, 1, {1003, 1004}, log_entry::entry_type::normal_with_blob));

    log_entries = read_log_file("pwal_0001", get_location());
    ASSERT_EQ(log_entries.size(), 1);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key3", "value3", 1, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    // online compaction
    run_compact_with_epoch_switch(5);

    // Check compaction catalog
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 4);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_TRUE(catalog.get_compacted_files().find(compacted_file_info("pwal_0000.compacted", 1)) != catalog.get_compacted_files().end());
    ASSERT_EQ(catalog.get_detached_pwals().size(), 2);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[0].find("pwal_0000.") == 0);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[1].find("pwal_0001.") == 0);

    // Check PWALs after compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 3);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    // Write entries without BLOBs and perform compaction.

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(1, "key15", "value5", {4, 2});
    lc2_->add_entry(1, "key16", "value6", {4, 3});
    lc2_->end_session();

    // Check PWALs before compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // online compaction
    run_compact_with_epoch_switch(6);

    // Check compaction catalog
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 5);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_TRUE(catalog.get_compacted_files().find(compacted_file_info("pwal_0000.compacted", 1)) != catalog.get_compacted_files().end());
    ASSERT_EQ(catalog.get_detached_pwals().size(), 3);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[0].find("pwal_0000.") == 0);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[1].find("pwal_0001.") == 0);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[2].find("pwal_0002.") == 0);

    // Check PWALs after compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 5);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key15", "value5", 0, 0, {1001, 1002}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key16", "value6", 0, 0, {1001, 1002}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));

    // Write entries with BLOBs but with blob_ids smaller than max_blob_id and perform compaction.

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(2, "key5", "value5", {4, 2}, {128, 32, 59});
    lc2_->end_session();

    // Check PWALs before compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // online compaction
    run_compact_with_epoch_switch(7);

    // Check compaction catalog
    catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 6);
    EXPECT_EQ(catalog.get_max_blob_id(), 1006);
    ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_TRUE(catalog.get_compacted_files().find(compacted_file_info("pwal_0000.compacted", 1)) != catalog.get_compacted_files().end());
    ASSERT_EQ(catalog.get_detached_pwals().size(), 4);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[0].find("pwal_0000.") == 0);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[1].find("pwal_0001.") == 0);
    EXPECT_TRUE(get_sorted_list(catalog.get_detached_pwals())[2].find("pwal_0002.") == 0);

    // Check PWALs after compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 2);

    log_entries = read_log_file("pwal_0000.compacted", get_location());
    ASSERT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, {1001, 1002}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key15", "value5", 0, 0, {1001, 1002}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key16", "value6", 0, 0, {1001, 1002}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key2", "value2", 0, 0, {1003, 1004}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 2, "key3", "value3", 0, 0, {1005, 1006}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key5", "value5", 0, 0, {128, 32, 59}, log_entry::entry_type::normal_with_blob));

    // datastoreの再起動後、コンパクションカタログのmax_blobg_idがnext_blob_idに反映されることを確認
    datastore_->shutdown();
    datastore_ = nullptr;
    gen_datastore();
    EXPECT_EQ(datastore_->next_blob_id(), 1007);
}

// This test is disabled because it is environment-dependent and may not work properly in CI environments.
TEST_F(compaction_test, DISABLED_fail_compact_with_io_error) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(get_location());
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // remove the file to cause an I/O error
    [[maybe_unused]] int result = std::system(("chmod 0500 " + std::string(get_location())).c_str());

    // First compaction.
    ASSERT_THROW(run_compact_with_epoch_switch(2), limestone_exception);
}

TEST_F(compaction_test, safe_rename_success) {
    boost::filesystem::path from = boost::filesystem::path(get_location()) / "test_file.txt";
    boost::filesystem::path to = boost::filesystem::path(get_location()) / "renamed_file.txt";

    boost::filesystem::ofstream ofs(from);
    ofs << "test content";
    ofs.close();

    ASSERT_NO_THROW(safe_rename(from, to));

    ASSERT_TRUE(boost::filesystem::exists(to));

    boost::filesystem::remove(to);
}

TEST_F(compaction_test, safe_rename_throws_exception) {
    boost::filesystem::path from = boost::filesystem::path(get_location()) / "non_existent_file.txt";
    boost::filesystem::path to = boost::filesystem::path(get_location()) / "renamed_file.txt";

    ASSERT_THROW(safe_rename(from, to), std::runtime_error);
}

TEST_F(compaction_test, select_files_for_compaction) {
    std::set<boost::filesystem::path> rotation_end_files = {boost::filesystem::path(get_location()) / "pwal_0001.0123456",
                                                            boost::filesystem::path(get_location()) / "pwal_0002.0123456",
                                                            boost::filesystem::path(get_location()) / "pwal_0003", boost::filesystem::path(get_location()) / "other_file"};
    std::set<std::string> detached_pwals = {"pwal_0002.0123456"};
    std::set<std::string> expected = {"pwal_0001.0123456"};

    std::set<std::string> result = select_files_for_compaction(rotation_end_files, detached_pwals);
    ASSERT_EQ(result, expected);
}

TEST_F(compaction_test, ensure_directory_exists_directory_exists) {
    boost::filesystem::path dir = boost::filesystem::path(get_location()) / "test_dir";
    boost::filesystem::create_directory(dir);

    ASSERT_NO_THROW(ensure_directory_exists(dir));
}

TEST_F(compaction_test, ensure_directory_exists_directory_created) {
    boost::filesystem::path dir = boost::filesystem::path(get_location()) / "test_dir";

    ASSERT_NO_THROW(ensure_directory_exists(dir));
    ASSERT_TRUE(boost::filesystem::exists(dir));
}

TEST_F(compaction_test, ensure_directory_exists_throws_exception) {
    boost::filesystem::path file = boost::filesystem::path(get_location()) / "test_file.txt";

    boost::filesystem::ofstream ofs(file);
    ofs.close();

    ASSERT_THROW(ensure_directory_exists(file), std::runtime_error);
}

TEST_F(compaction_test, ensure_directory_exists_parent_directory_missing) {
    boost::filesystem::path dir = boost::filesystem::path(get_location()) / "nonexistent_parent/test_dir";
    ASSERT_THROW(ensure_directory_exists(dir), std::runtime_error);
}

TEST_F(compaction_test, handle_existing_compacted_file_no_existing_files) {
    boost::filesystem::path location_path = boost::filesystem::path(get_location());

    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
}

TEST_F(compaction_test, handle_existing_compacted_file_with_existing_file) {
    boost::filesystem::path location_path = boost::filesystem::path(get_location());
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::ofstream ofs(compacted_file);
    ofs.close();

    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
    ASSERT_TRUE(boost::filesystem::exists(location_path / "pwal_0000.compacted.prev"));
}

TEST_F(compaction_test, handle_existing_compacted_file_throws_exception) {
    boost::filesystem::path location_path = boost::filesystem::path(get_location());
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::path compacted_prev_file = location_path / "pwal_0000.compacted.prev";
    boost::filesystem::ofstream ofs1(compacted_file);
    ofs1.close();
    boost::filesystem::ofstream ofs2(compacted_prev_file);
    ofs2.close();

    ASSERT_THROW(handle_existing_compacted_file(location_path), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory) {
    boost::filesystem::path test_dir = boost::filesystem::path(get_location());
    boost::filesystem::path file1 = test_dir / "file1.txt";
    boost::filesystem::path file2 = test_dir / "file2.txt";
    boost::filesystem::ofstream ofs1(file1);
    ofs1.close();
    boost::filesystem::ofstream ofs2(file2);
    ofs2.close();

    std::set<std::string> expected_files = {"file1.txt", "file2.txt"};

    std::set<std::string> files = get_files_in_directory(test_dir);
    EXPECT_EQ(files, expected_files);
}

TEST_F(compaction_test, get_files_in_directory_directory_not_exists) {
    boost::filesystem::path non_existent_dir = boost::filesystem::path(get_location()) / "non_existent_dir";
    ASSERT_THROW(get_files_in_directory(non_existent_dir), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory_not_a_directory) {
    boost::filesystem::path file_path = boost::filesystem::path(get_location()) / "test_file.txt";
    boost::filesystem::ofstream ofs(file_path);
    ofs.close();

    ASSERT_THROW(get_files_in_directory(file_path), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory_with_files) {
    boost::filesystem::path test_dir = boost::filesystem::path(get_location()) / "test_dir";
    boost::filesystem::create_directory(test_dir);

    boost::filesystem::path file1 = test_dir / "file1.txt";
    boost::filesystem::path file2 = test_dir / "file2.txt";
    boost::filesystem::ofstream ofs1(file1);
    boost::filesystem::ofstream ofs2(file2);

    std::set<std::string> expected_files = {"file1.txt", "file2.txt"};
    std::set<std::string> files = get_files_in_directory(test_dir);

    EXPECT_EQ(files, expected_files);
}

TEST_F(compaction_test, get_files_in_directory_empty_directory) {
    boost::filesystem::path empty_dir = boost::filesystem::path(get_location()) / "empty_test_dir";
    boost::filesystem::create_directory(empty_dir);

    std::set<std::string> files = get_files_in_directory(empty_dir);
    EXPECT_TRUE(files.empty());
}

TEST_F(compaction_test, remove_file_safely_success) {
    boost::filesystem::path file = boost::filesystem::path(get_location()) / "test_file_to_remove.txt";

    {
        boost::filesystem::ofstream ofs(file);
        ofs << "test content";
    }

    ASSERT_TRUE(boost::filesystem::exists(file));
    ASSERT_NO_THROW(remove_file_safely(file));
    ASSERT_FALSE(boost::filesystem::exists(file));
}

TEST_F(compaction_test, remove_file_safely_no_exception_for_nonexistent_file) {
    boost::filesystem::path file = boost::filesystem::path(get_location()) / "non_existent_file.txt";
    ASSERT_NO_THROW(remove_file_safely(file));
}

// This test is disabled because it is environment-dependent and may not work properly in CI environments.
TEST_F(compaction_test, DISABLED_remove_file_safely_fails_to_remove_file) {
    boost::filesystem::path test_dir = boost::filesystem::path(get_location());
    boost::filesystem::path file = test_dir / "protected_file.txt";

    boost::filesystem::ofstream ofs(file);
    ofs << "This file is protected and cannot be removed.";
    ofs.close();
    chmod(test_dir.string().c_str(), 0444);

    ASSERT_THROW(remove_file_safely(file), std::runtime_error);

    chmod(test_dir.string().c_str(), 0755);
    boost::filesystem::remove(file);
}

}  // namespace limestone::testing
