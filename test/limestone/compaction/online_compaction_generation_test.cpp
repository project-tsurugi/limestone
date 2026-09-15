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

#include <condition_variable>
#include <fstream>
#include <future>
#include <mutex>

#include <limestone/api/limestone_exception.h>

#include "compaction_test_fixture.h"
#include "datastore_impl.h"

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

    // Runs compact_with_online on a background thread, freezes it at the given phase
    // boundary hook (freeze_after_commit = false: after the publish and before the
    // commit / true: after the commit and before the old-generation removal), copies
    // the whole log directory to copy_to at that point, and then releases the thread.
    // The copied state is equivalent to a log directory that crashed at that phase
    // boundary.
    void capture_crash_state_at_phase_boundary(bool freeze_after_commit, epoch_id_type next_epoch,
                                               const boost::filesystem::path& copy_to) {
        auto* test_datastore = dynamic_cast<datastore_test*>(datastore_.get());
        ASSERT_NE(test_datastore, nullptr);

        std::mutex mtx;
        std::condition_variable cv;
        bool rotation_reached = false;
        bool hook_reached = false;
        bool released = false;

        test_datastore->on_rotate_log_files_callback = [&]() {
            {
                std::lock_guard<std::mutex> lock(mtx);
                rotation_reached = true;
            }
            cv.notify_all();
        };
        auto freeze_hook = [&]() {
            std::unique_lock<std::mutex> lock(mtx);
            hook_reached = true;
            cv.notify_all();
            cv.wait(lock, [&] { return released; });
        };
        if (freeze_after_commit) {
            datastore_->get_impl()->set_on_compaction_after_commit_for_test(freeze_hook);
        } else {
            datastore_->get_impl()->set_on_compaction_after_publish_for_test(freeze_hook);
        }

        auto future = std::async(std::launch::async, [this] { datastore_->compact_with_online(); });
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return rotation_reached; });
        }
        datastore_->switch_epoch(next_epoch);
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return hook_reached; });
        }
        copy_dir_recursive(boost::filesystem::path(location), copy_to);
        {
            std::lock_guard<std::mutex> lock(mtx);
            released = true;
        }
        cv.notify_all();
        future.get();
        datastore_->get_impl()->set_on_compaction_after_publish_for_test(nullptr);
        datastore_->get_impl()->set_on_compaction_after_commit_for_test(nullptr);
        test_datastore->on_rotate_log_files_callback = nullptr;
    }

    // Puts the captured crash state back into location so that a datastore can start on it.
    void restore_crash_state(const boost::filesystem::path& copy_from) {
        datastore_->shutdown();
        datastore_ = nullptr;
        boost::filesystem::remove_all(boost::filesystem::path(location));
        copy_dir_recursive(copy_from, boost::filesystem::path(location));
        boost::filesystem::remove_all(copy_from);
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

// Idempotency of the double read: in the one-time migration window (a crash remnant of
// an old binary, where the generation-0 compacted file registered by the catalog and the
// un-detached originals hold the same entries), the duplicate of a key with an equal
// value is read from both the compacted file and the original, and the result converges
// to a single entry.
TEST_F(online_compaction_generation_test, double_read_of_migration_window_is_idempotent) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Build the migration-window state by hand: rename compacted.1 to the unnamed
    // (generation 0) form while keeping its registration, and empty the detached record
    // so that the original pwal becomes a live input again. k1 then appears both in the
    // compacted file (write version reset to 0) and in the original (real write version).
    boost::filesystem::path dir{location};
    boost::filesystem::rename(dir / "pwal_0000.compacted.1",
                              dir / compaction_catalog::get_compacted_filename());
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(dir);
        compaction_catalog rewritten{dir};
        rewritten.update_catalog_file(catalog.get_max_epoch_id(), catalog.get_max_blob_id(), 0,
                                      {{compaction_catalog::get_compacted_filename(), 1}},
                                      std::nullopt, {});
    }

    gen_datastore();

    // The duplicate with an equal value converges to a single entry.
    std::unique_ptr<snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<cursor> cursor = snapshot->get_cursor();
    std::vector<std::pair<std::string, std::string>> kv_list;
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        kv_list.emplace_back(key, value);
    }
    ASSERT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
}

// A crash between the publish and the commit: the new generation (published but not
// recorded by the catalog) is removed as orphans at startup, and the catalog stays fully
// consistent, pointing at the old generation and the un-detached originals.
TEST_F(online_compaction_generation_test, orphan_new_generation_from_publish_commit_crash_is_removed_at_startup) {
    boost::filesystem::path crash_copy{std::string(location) + "_crash"};
    boost::filesystem::remove_all(crash_copy);

    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);  // generation 1
    write_and_make_durable("k2", "v2", 3);

    // Freeze the generation-2 compaction right after the publish (before the commit)
    // and capture the crash state.
    capture_crash_state_at_phase_boundary(false, 5, crash_copy);

    // The captured state: the new-generation file is published but the catalog still points at the old generation.
    ASSERT_TRUE(boost::filesystem::exists(crash_copy / "pwal_0000.compacted.2"));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(crash_copy);
        EXPECT_EQ(catalog.get_generation(), 1);
    }

    restore_crash_state(crash_copy);
    gen_datastore();

    // The new generation is removed as an orphan and the catalog stays at generation 1.
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000.compacted.2"));
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000.compacted.1"));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 1);
    }

    // The original of k2 has not become a detached pwal, so all data is readable.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_EQ(kv_list.size(), 2);
}

// A crash between the commit and the old-generation removal: the old generation (no
// longer in the catalog) is removed as an orphan at startup, and the state is consistent
// with the new generation and the detached records.
TEST_F(online_compaction_generation_test, orphan_old_generation_from_commit_removal_crash_is_removed_at_startup) {
    boost::filesystem::path crash_copy{std::string(location) + "_crash"};
    boost::filesystem::remove_all(crash_copy);

    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);  // generation 1
    write_and_make_durable("k2", "v2", 3);

    // Freeze the generation-2 compaction right after the commit (before the
    // old-generation removal) and capture the crash state.
    capture_crash_state_at_phase_boundary(true, 5, crash_copy);

    // The captured state: the catalog points at the new generation but the old-generation file still remains.
    ASSERT_TRUE(boost::filesystem::exists(crash_copy / "pwal_0000.compacted.1"));
    ASSERT_TRUE(boost::filesystem::exists(crash_copy / "pwal_0000.compacted.2"));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(crash_copy);
        EXPECT_EQ(catalog.get_generation(), 2);
    }

    restore_crash_state(crash_copy);
    gen_datastore();

    // The old generation is removed as an orphan and the new generation stays current.
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000.compacted.1"));
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000.compacted.2"));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(boost::filesystem::path(location));
        EXPECT_EQ(catalog.get_generation(), 2);
    }

    // All data is readable from the compacted file of the new generation.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_EQ(kv_list.size(), 2);
}

// Migration startup: starting from a crash remnant of an old binary (a ".prev" file
// plus the registered generation-0 file), the ".prev" file and an unregistered
// carry-named remnant are removed as orphans, the registered generation-0 file stays,
// and the data is restored correctly.
TEST_F(online_compaction_generation_test, prev_remnant_and_unregistered_carry_are_removed_at_startup) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Build the generation-0 migration state by hand, and place a ".prev" remnant of
    // the retired scheme and a file with an unregistered carry name.
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
    boost::filesystem::path prev_path = dir / (compaction_catalog::get_compacted_filename() + ".prev");
    boost::filesystem::copy_file(dir / compaction_catalog::get_compacted_filename(), prev_path);
    boost::filesystem::path orphan_carry_path = dir / "pwal_0000.carry.7";
    boost::filesystem::copy_file(dir / compaction_catalog::get_compacted_filename(), orphan_carry_path);

    gen_datastore();

    // The ".prev" file and the unregistered carry are removed; the registered generation-0 file stays.
    EXPECT_FALSE(boost::filesystem::exists(prev_path));
    EXPECT_FALSE(boost::filesystem::exists(orphan_carry_path));
    EXPECT_TRUE(boost::filesystem::exists(dir / compaction_catalog::get_compacted_filename()));

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
}

// The outputs a crash during the write phase left in the temporary directory are
// cleaned up at startup (leaving them cannot affect the data, since they never appear
// directly under the log directory, but they would leak disk space).
TEST_F(online_compaction_generation_test, compaction_temp_remnants_are_cleaned_at_startup) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Mimic the remnant of a crash during the write phase.
    boost::filesystem::path temp_dir =
        boost::filesystem::path(location) / compaction_catalog::get_compaction_temp_dirname();
    boost::filesystem::create_directories(temp_dir);
    { std::ofstream ofs((temp_dir / "pwal_0000.compacted.1").string()); ofs << "half-written"; }

    gen_datastore();

    EXPECT_FALSE(boost::filesystem::exists(temp_dir));

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
}

// Starting from the crash window of a catalog update (no catalog, backup present)
// recovers the catalog from the backup, and the recorded compacted file is not removed
// as an orphan. This pins the hole where creating a fresh empty catalog would make the
// backup ignored forever and let the orphan removal delete the legitimate compacted
// file, losing data permanently.
TEST_F(online_compaction_generation_test, catalog_backup_is_recovered_at_startup) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);  // generation 1
    datastore_->shutdown();
    datastore_ = nullptr;

    // Build the crash window of a catalog update: the state right after the catalog
    // was renamed to the backup.
    boost::filesystem::path dir{location};
    boost::filesystem::path catalog_path = dir / compaction_catalog::get_catalog_filename();
    boost::filesystem::path backup_path = dir / compaction_catalog::get_catalog_backup_filename();
    boost::filesystem::rename(catalog_path, backup_path);
    ASSERT_FALSE(boost::filesystem::exists(catalog_path));

    gen_datastore();

    // The catalog is recovered from the backup and the recorded compacted file remains
    // (had an empty catalog been created, it would have been removed as an orphan).
    EXPECT_TRUE(boost::filesystem::exists(dir / "pwal_0000.compacted.1"));
    EXPECT_TRUE(boost::filesystem::exists(catalog_path));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(dir);
        EXPECT_EQ(catalog.get_generation(), 1);
        ASSERT_TRUE(catalog.get_current_compacted_file_name().has_value());
        EXPECT_EQ(catalog.get_current_compacted_file_name().value(), "pwal_0000.compacted.1");
    }

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
}

// A failure of the orphan removal (forced by a non-empty directory under an orphan
// name, on which remove fails) makes the startup fail.
TEST_F(online_compaction_generation_test, startup_fails_when_orphan_removal_fails) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);
    run_compact_with_epoch_switch(3);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Place a non-empty directory under an orphan name (remove fails with ENOTEMPTY).
    boost::filesystem::path orphan_dir = boost::filesystem::path(location) / "pwal_0000.compacted.99";
    boost::filesystem::create_directories(orphan_dir);
    { std::ofstream ofs((orphan_dir / "blocker").string()); ofs << "x"; }

    EXPECT_THROW(gen_datastore(), limestone_exception);

    // Cleanup (no datastore was created, so TearDown can remove the location).
    boost::filesystem::remove_all(orphan_dir);
}

} // namespace limestone::testing
