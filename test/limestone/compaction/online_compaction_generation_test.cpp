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

#include "compaction_test_fixture.h"

namespace limestone::testing {

// Tests for the generation-number scheme of the online compaction: the monotonic
// increase of the generation number, the record of the current generation name in the
// catalog, the removal of the old-generation files, the absence of a carry file when
// there is no snippet beyond the boundary, and the retry after a failed temporary write.
class online_compaction_generation_test : public compaction_test {
public:
    online_compaction_generation_test() : compaction_test("/tmp/online_compaction_generation_test") {}

    void write_and_make_durable(const std::string& key, const std::string& value, epoch_id_type write_epoch) {
        lc0_->begin_session();
        lc0_->add_entry(1, key, value, {write_epoch, 0});
        lc0_->end_session();
        datastore_->switch_epoch(write_epoch + 1);
    }
};

// The generation number is incremented by one per compaction, the catalog records the
// file name of the current generation, and the compacted file of the old generation is
// removed. No carry file is created when there is no snippet beyond the boundary.
TEST_F(online_compaction_generation_test, generation_increments_and_old_generation_is_deleted) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);

    run_compact_with_epoch_switch(3);

    std::string first_name;
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 1);
        ASSERT_TRUE(catalog.get_current_compacted_file_name().has_value());
        first_name = catalog.get_current_compacted_file_name().value();
        EXPECT_EQ(first_name, "pwal_0000.compacted.1");
        EXPECT_FALSE(catalog.get_carry_file().has_value());
    }
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / first_name));
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000.carry.1"));

    write_and_make_durable("k2", "v2", 3);
    run_compact_with_epoch_switch(5);

    std::string second_name;
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 2);
        ASSERT_TRUE(catalog.get_current_compacted_file_name().has_value());
        second_name = catalog.get_current_compacted_file_name().value();
        EXPECT_EQ(second_name, "pwal_0000.compacted.2");
    }
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / second_name));
    // The old generation is removed (and is not registered as a detached pwal either).
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / first_name));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_detached_pwals().count(first_name), 0);
    }

    // All data is still readable after a restart.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_EQ(kv_list.size(), 2);
}

// Migration: the first compaction from the catalog of an existing deployment, which
// records generation 0 (the unnamed pwal_0000.compacted), switches to the name of
// generation 1 and removes the unnamed old file.
TEST_F(online_compaction_generation_test, migrates_from_generation_zero_legacy_name) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Build the generation-0 state by hand: rename compacted.1 to the unnamed form and
    // rewrite the catalog with generation 0 and the unnamed record, which is the shape of
    // an existing deployment right after the migration.
    boost::filesystem::path dir{location};
    boost::filesystem::rename(dir / "pwal_0000.compacted.1",
                              dir / compaction_catalog::get_compacted_filename());
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(dir);
        compaction_catalog rewritten{dir};
        rewritten.update_catalog_file(catalog.get_max_epoch_id(), catalog.get_max_blob_id(), 0,
                                      {{compaction_catalog::get_compacted_filename(), 1}},
                                      std::nullopt, catalog.get_detached_pwals());
    }

    gen_datastore();
    datastore_->switch_epoch(4);
    write_and_make_durable("k2", "v2", 4);
    run_compact_with_epoch_switch(6);

    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(dir);
        EXPECT_EQ(catalog.get_generation(), 1);
        ASSERT_TRUE(catalog.get_current_compacted_file_name().has_value());
        EXPECT_EQ(catalog.get_current_compacted_file_name().value(), "pwal_0000.compacted.1");
    }
    // The unnamed old generation is removed and does not enter detached_pwals either.
    EXPECT_FALSE(boost::filesystem::exists(dir / compaction_catalog::get_compacted_filename()));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(dir);
        EXPECT_EQ(catalog.get_detached_pwals().count(compaction_catalog::get_compacted_filename()), 0);
    }

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_EQ(kv_list.size(), 2);
}

// A failed temporary write, forced by placing a directory in the temporary directory
// under the same name as the output so that fopen fails, lets the process continue,
// leaves the catalog unchanged, and succeeds on the next compaction request.
TEST_F(online_compaction_generation_test, temp_write_failure_is_retried_on_the_next_request) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);

    // Place an empty directory under the same name as the temporary file that the next
    // compaction writes (generation 1).
    boost::filesystem::path temp_dir =
        boost::filesystem::path(location) / compaction_catalog::get_compaction_temp_dirname();
    boost::filesystem::create_directories(temp_dir / "pwal_0000.compacted.1");

    run_compact_with_epoch_switch(3);

    // The compaction has not run (the catalog stays in its initial state).
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 0);
        EXPECT_TRUE(catalog.get_compacted_files().empty());
    }
    // The half-written temporary file is removed (the directory placed above disappears
    // with that cleanup).
    EXPECT_FALSE(boost::filesystem::exists(temp_dir / "pwal_0000.compacted.1"));

    // The next request succeeds.
    run_compact_with_epoch_switch(4);
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 1);
        ASSERT_TRUE(catalog.get_current_compacted_file_name().has_value());
        EXPECT_EQ(catalog.get_current_compacted_file_name().value(), "pwal_0000.compacted.1");
    }

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
}

} // namespace limestone::testing
