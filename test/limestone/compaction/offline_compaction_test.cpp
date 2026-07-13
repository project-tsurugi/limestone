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

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <limestone/api/limestone_exception.h>

#include "compaction_test_fixture.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

// Test fixture for offline compaction (tglogutil compaction). It reuses the
// datastore-based helpers of compaction_test and adds a helper to drive the
// external tglogutil binary, so that the whole offline-compaction round trip
// (datastore -> shutdown -> offline compaction -> restart) can be exercised.
class offline_compaction_test : public compaction_test {
public:
    // Place the log directory under a per-suite directory (rather than directly under
    // /tmp) so that this suite does not collide with compaction_test, which shares the
    // same fixture base and runs in parallel under ctest. Offline compaction creates its
    // work/backup directories next to the log directory (see make_tmp_dir_next_to), so
    // keeping the log directory one level down confines those siblings to test_root_ and
    // keeps find_sibling_dirs from scanning /tmp itself, where unrelated processes churn
    // temporary files.
    static constexpr char const* test_root_ = "/tmp/offline_compaction_test";
    offline_compaction_test() : compaction_test("/tmp/offline_compaction_test/log_dir") {}

    // Recreate the per-suite root before each test and remove it afterwards. Wiping the
    // whole root also removes any .work_XXXXXX / .backup_XXXXXX siblings, including ones
    // left behind by a test that aborted before its own cleanup, so a later test can never
    // observe a stale working or backup directory.
    void SetUp() override {
        boost::filesystem::remove_all(test_root_);
        boost::filesystem::create_directories(test_root_);
        compaction_test::SetUp();
    }

    void TearDown() override {
        compaction_test::TearDown();
        boost::filesystem::remove_all(test_root_);
    }

    // Path to the offline compaction utility (tglogutil), relative to the test
    // executable's working directory. This matches the convention used by
    // dblogutil_compaction_test.
    static constexpr char const* util_command = "../src/tglogutil";

    // Invoke an external command and capture its combined output.
    static int invoke(const std::string& command, std::string& out) {
        FILE* fp = popen(command.c_str(), "r");
        if (fp == nullptr) {
            out = std::string("popen failed: ") + strerror(errno);
            return -1;
        }
        std::array<char, 4096> buf{};
        std::ostringstream ss;
        std::size_t rc = 0;
        while ((rc = fread(buf.data(), 1, buf.size() - 1, fp)) > 0) {
            ss.write(buf.data(), static_cast<std::streamsize>(rc));
        }
        out.assign(ss.str());
        LOG(INFO) << "\n" << out;
        return pclose(fp);
    }

    // Run offline compaction on the test location via the tglogutil binary.
    // Extra command line options (e.g. "--make_backup") can be passed through.
    void run_offline_compaction(std::string const& extra_options = "") {
        std::string out;
        std::string command = std::string(util_command) + " compaction --force " +
            (extra_options.empty() ? "" : extra_options + " ") + std::string(location) + " 2>&1";
        int rc = invoke(command, out);
        ASSERT_EQ(rc, 0) << "invoke failed: " << out;
        ASSERT_TRUE(out.find("compaction was successfully completed: ") != std::string::npos)
            << "tglogutil output:\n" << out;
    }

    // Read a whole file into a string (byte-exact, for content comparison).
    static std::string read_file(boost::filesystem::path const& path) {
        std::ifstream ifs(path.string(), std::ios::binary);
        std::ostringstream ss;
        ss << ifs.rdbuf();
        return ss.str();
    }

    // Enumerate sibling directories of the test location whose name is
    // "<location><marker>XXXXXX" (tglogutil creates ".work_" / ".backup_" directories
    // next to the target directory).
    std::vector<boost::filesystem::path> find_sibling_dirs(std::string const& marker) const {
        std::vector<boost::filesystem::path> dirs;
        boost::filesystem::path base{location};
        std::string prefix = base.filename().string() + marker;
        for (boost::filesystem::directory_iterator it{base.parent_path()}, end; it != end; ++it) {
            if (boost::filesystem::is_directory(it->status()) &&
                starts_with(it->path().filename().string(), prefix)) {
                dirs.push_back(it->path());
            }
        }
        return dirs;
    }

    // Enumerate backup directories created by "tglogutil compaction --make_backup".
    std::vector<boost::filesystem::path> find_backup_dirs() const {
        return find_sibling_dirs(".backup_");
    }

    // Remove leftover backup directories (from this run or an earlier aborted one).
    void remove_backup_dirs() const {
        for (boost::filesystem::path const& dir : find_backup_dirs()) {
            boost::filesystem::remove_all(dir);
        }
    }

    // Remove leftover working directories that a failed compaction run leaves behind.
    void remove_work_dirs() const {
        for (boost::filesystem::path const& dir : find_sibling_dirs(".work_")) {
            boost::filesystem::remove_all(dir);
        }
    }

    // Collect the set of top-level entry names (files and directories) directly under dir.
    static std::set<std::string> list_top_level_entries(boost::filesystem::path const& dir) {
        std::set<std::string> names;
        for (boost::filesystem::directory_iterator it{dir}, end; it != end; ++it) {
            names.insert(it->path().filename().string());
        }
        return names;
    }

    // Decide whether a top-level entry is expected to disappear during compaction.
    // This is an explicit allow-list of the entries compaction is known to consume:
    // it merges the pwal files into pwal_0000.compacted, rewrites the epoch file, and
    // drops derived/temporary artifacts. Anything not listed here - including files a
    // future change might add to the log directory - is treated as "must be preserved",
    // so that a compaction that fails to handle such a new file is detected as a lost file.
    bool is_expected_to_disappear(std::string const& name) const {
        // rotated epoch files (epoch.<ts>.<id>): the durable epoch is re-emitted into "epoch"
        if (starts_with(name, "epoch.")) {
            return true;
        }
        // the temporary epoch file
        if (name == ".epoch.tmp") {
            return true;
        }
        // Derived / control-only directories that the datastore recreates on startup:
        // - compaction_temp: online-compaction working directory
        // - data: the snapshot directory
        // - ctrl: online-compaction control directory (holds the start_compaction trigger)
        if (name == compaction_catalog::get_compaction_temp_dirname() || name == "data"
            || name == "ctrl") {
            return true;
        }
        // pwal files other than the compacted ones are merged into pwal_0000.compacted
        if (starts_with(name, "pwal_")) {
            return name != compacted_filename && name != (compacted_filename + ".prev");
        }
        return false;
    }

    // Take a recursive snapshot of dir: a map from the path relative to dir to the file
    // content. Directories are recorded with an empty content so that empty directories
    // are compared as well.
    static std::map<std::string, std::string> snapshot_tree(boost::filesystem::path const& dir) {
        std::map<std::string, std::string> tree;
        for (boost::filesystem::recursive_directory_iterator it{dir}, end; it != end; ++it) {
            std::string rel = boost::filesystem::relative(it->path(), dir).string();
            if (boost::filesystem::is_directory(it->status())) {
                tree.emplace(rel, std::string{});
            } else {
                tree.emplace(rel, read_file(it->path()));
            }
        }
        return tree;
    }
};

// Reproduces tsurugi-issues #1498.
//
// After offline compaction, the compaction catalog must register the compacted
// file. Otherwise, a later restart that needs to apply remove entries (e.g. a
// deleted record) generates a snapshot that drops the remove entries, and the
// deleted record stored in pwal_0000.compacted is resurrected.
TEST_F(offline_compaction_test, preserves_remove_entries_after_restart) {

    // 1. Create initial records and shut down the datastore.
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "A", "va", {1, 0});
    lc0_->add_entry(1, "B", "vb", {1, 1});
    lc0_->add_entry(1, "C", "vc", {1, 2});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    // 2. Perform offline compaction with the tglogutil binary.
    run_offline_compaction();

    // The compaction catalog must register the compacted file (this is the fix
    // for #1498; without it, get_compacted_files() is empty).
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        const std::set<compacted_file_info>& compacted_files = catalog.get_compacted_files();
        // The catalog must register exactly one compacted file: pwal_0000.compacted (version 1).
        // Use EXPECT (not ASSERT) so that the end-to-end snapshot checks below still run
        // even if this catalog check fails, which is what actually reproduces the field issue.
        EXPECT_EQ(compacted_files.size(), 1);
        if (!compacted_files.empty()) {
            const compacted_file_info& info = *compacted_files.begin();
            EXPECT_EQ(info.get_file_name(), compacted_filename);
            EXPECT_EQ(info.get_version(), 1);
        }
    }
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / compacted_filename));

    // 3. Restart, delete record "B" (emits a remove entry), and shut down.
    gen_datastore();
    datastore_->switch_epoch(3);
    lc0_->begin_session();
    lc0_->remove_entry(1, "B", {3, 0});
    lc0_->end_session();
    datastore_->switch_epoch(4);

    // 4. Restart and read the snapshot. "B" must stay deleted; "A" and "C" must
    //    remain. With the bug, "B" is resurrected from pwal_0000.compacted.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    std::map<std::string, std::string> kv;
    for (const auto& [key, value] : kv_list) {
        kv.emplace(key, value);
    }

    EXPECT_EQ(kv.size(), 2);
    EXPECT_EQ(kv.count("B"), 0u);  // must not be resurrected
    ASSERT_EQ(kv.count("A"), 1u);
    ASSERT_EQ(kv.count("C"), 1u);
    EXPECT_EQ(kv["A"], "va");
    EXPECT_EQ(kv["C"], "vc");
}

// Verifies that startup fails fast when the compaction catalog is inconsistent
// with the WAL files: the compacted file exists on disk but is not registered in
// the catalog (the broken state produced by the #1498 bug).
TEST_F(offline_compaction_test, detects_inconsistent_compaction_catalog_at_startup) {

    // Produce a valid compacted file and catalog via online compaction.
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "A", "va", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    run_compact_with_epoch_switch(3);

    boost::filesystem::path compacted_path = boost::filesystem::path(location) / compacted_filename;
    ASSERT_TRUE(boost::filesystem::exists(compacted_path));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        ASSERT_EQ(catalog.get_compacted_files().size(), 1);
    }

    datastore_->shutdown();
    datastore_ = nullptr;

    // Corrupt the catalog: drop the compacted-file registration while the
    // compacted file itself remains on disk.
    {
        compaction_catalog catalog{location};
        catalog.update_catalog_file(0, 0, {}, {});
    }
    ASSERT_TRUE(boost::filesystem::exists(compacted_path));

    // Startup must fail fast because the catalog is inconsistent. Verify both that a
    // limestone_exception is thrown and that its message is the intended one (so that an
    // unrelated failure that happens to throw the same type is not mistaken for success).
    // This substring must match the message produced by datastore startup.
    const std::string expected_message_substr = "compaction catalog is inconsistent";
    try {
        gen_datastore();
        FAIL() << "expected a limestone_exception to be thrown, but nothing was thrown";
    } catch (const limestone_exception& e) {
        EXPECT_NE(std::string(e.what()).find(expected_message_substr), std::string::npos)
            << "unexpected exception message: " << e.what();
    } catch (const std::exception& e) {
        FAIL() << "expected a limestone_exception, but a different exception was thrown: " << e.what();
    }
}

// Offline compaction must preserve the max_blob_id high-water mark recorded in the
// existing catalog, even though the freshly computed value (no blobs here) is lower.
// Lowering it could lead to blob-id reuse.
TEST_F(offline_compaction_test, offline_compaction_preserves_blob_id_high_water_mark) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});  // no blob -> computed max_blob_id is 0
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Simulate a high-water mark left by earlier blob allocations / online compactions.
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        catalog.update_catalog_file(catalog.get_max_epoch_id(), 9999, {}, {});
    }

    run_offline_compaction();

    // The high-water mark must survive offline compaction (not lowered to the computed 0).
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_blob_id(), 9999);
}

// Offline compaction must recover the existing catalog from its backup when the main
// catalog file is unreadable, and still preserve the high-water mark.
TEST_F(offline_compaction_test, offline_compaction_recovers_catalog_from_backup) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        catalog.update_catalog_file(catalog.get_max_epoch_id(), 7777, {}, {});
    }

    // Leave the valid content only in the backup, then corrupt the main catalog file.
    boost::filesystem::path dir{location};
    boost::filesystem::path main_catalog = dir / compaction_catalog::get_catalog_filename();
    boost::filesystem::path backup_catalog = dir / compaction_catalog::get_catalog_backup_filename();
    boost::filesystem::copy_file(main_catalog, backup_catalog, boost::filesystem::copy_options::overwrite_existing);
    {
        std::ofstream ofs(main_catalog.string(), std::ios::trunc);
        ofs << "GARBAGE";
    }

    run_offline_compaction();

    // The catalog must have been recovered from the backup and the high-water mark kept.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_blob_id(), 7777);
}

// Offline compaction must not lose the blob data: the blob directory has to be carried
// over from the original log directory to the compacted one. With the bug, the whole
// blob directory was deleted together with the original log directory.
TEST_F(offline_compaction_test, offline_compaction_preserves_blob_files) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->end_session();
    boost::filesystem::path blob_path = create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    ASSERT_TRUE(boost::filesystem::exists(blob_path));

    run_offline_compaction();

    // The blob file must physically survive offline compaction (default mode: moved).
    EXPECT_TRUE(boost::filesystem::exists(blob_path));

    // The datastore must restart and still see the blob-referencing entry and the blob file.
    gen_datastore();
    std::unique_ptr<snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<cursor> cursor = snapshot->get_cursor();
    std::map<std::string, std::string> kv;
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        kv.emplace(key, value);
    }
    ASSERT_EQ(kv.count("blob_key"), 1U);
    EXPECT_EQ(kv["blob_key"], "blob_value");
    EXPECT_TRUE(boost::filesystem::exists(blob_path));
}

// Offline compaction must keep the original manifest file so that the instance_uuid
// and the persistent format version survive. With the bug, a brand-new manifest with
// a freshly generated instance_uuid replaced the original one.
TEST_F(offline_compaction_test, offline_compaction_preserves_manifest_file) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    std::string manifest_before = read_file(manifest_path);
    ASSERT_FALSE(manifest_before.empty());
    ASSERT_NE(manifest_before.find("instance_uuid"), std::string::npos);

    run_offline_compaction();

    // The manifest must be carried over byte-exact (same instance_uuid, same versions).
    std::string manifest_after = read_file(manifest_path);
    EXPECT_EQ(manifest_after, manifest_before);
}

// Dry run must not modify the log directory at all. The command reports whether
// compaction would succeed, but leaves the original directory byte-for-byte
// unchanged and creates no backup or leftover working directory. This covers the
// whole directory (including the blob directory), not just the transaction log
// files, so a regression that starts touching from_dir during a dry run is caught.
TEST_F(offline_compaction_test, offline_compaction_dry_run_leaves_log_directory_intact) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->end_session();
    create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    std::map<std::string, std::string> before = snapshot_tree(location);

    std::string out;
    std::string command = std::string(util_command) + " compaction --force --dry_run " +
        std::string(location) + " 2>&1";
    int rc = invoke(command, out);
    ASSERT_EQ(rc, 0) << "invoke failed: " << out;
    EXPECT_TRUE(out.find("compaction will be successfully completed (dry-run mode)") != std::string::npos)
        << "tglogutil output:\n" << out;

    // The log directory must be byte-for-byte identical to its pre-run state.
    EXPECT_EQ(snapshot_tree(location), before);

    // A dry run must not create a backup or leave a working directory behind.
    EXPECT_TRUE(find_backup_dirs().empty());
    EXPECT_TRUE(find_sibling_dirs(".work_").empty());

    remove_backup_dirs();
    remove_work_dirs();
}

// The --epoch option is ignored by the compaction subcommand: it never restricts
// which entries are kept. Even an --epoch below the durable epoch must not drop
// any durable data. Record "B" is written in epoch 2, above the specified
// --epoch=1, so an implementation that honored --epoch as an upper limit on valid
// epochs would discard it; the test fails against such an implementation.
TEST_F(offline_compaction_test, offline_compaction_ignores_epoch_option) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "A", "va", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    lc0_->begin_session();
    lc0_->add_entry(1, "B", "vb", {2, 0});
    lc0_->end_session();
    datastore_->switch_epoch(3);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Compact with an --epoch below the epoch of record "B". It must be ignored, so
    // both records survive.
    run_offline_compaction("--epoch=1");

    // Reopen the datastore so restart_datastore_and_read_snapshot() (which shuts the
    // current datastore down before reopening) has a live instance to work with.
    gen_datastore();
    std::vector<std::pair<std::string, std::string>> kv_list =
        restart_datastore_and_read_snapshot();
    std::map<std::string, std::string> kv;
    for (const auto& [key, value] : kv_list) {
        kv.emplace(key, value);
    }

    ASSERT_EQ(kv.size(), 2u);
    ASSERT_EQ(kv.count("A"), 1u);
    ASSERT_EQ(kv.count("B"), 1u);
    EXPECT_EQ(kv["A"], "va");
    EXPECT_EQ(kv["B"], "vb");
}

// Options belonging to other subcommands are ignored without even being validated.
// A malformed --epoch value must not make the compaction subcommand fail; only
// inspect and repair parse the option.
TEST_F(offline_compaction_test, offline_compaction_ignores_malformed_epoch_option) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "A", "va", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    // run_offline_compaction() asserts that the command exits with status 0; a
    // malformed --epoch must not be rejected here.
    run_offline_compaction("--epoch=invalid");

    gen_datastore();
    std::vector<std::pair<std::string, std::string>> kv_list =
        restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 1u);
    EXPECT_EQ(kv_list[0].first, "A");
    EXPECT_EQ(kv_list[0].second, "va");
}

// With --make_backup, the blob directory must be copied (not moved) so that both the
// compacted log directory and the backup directory keep the blob data.
TEST_F(offline_compaction_test, offline_compaction_with_backup_copies_blob_directory) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->end_session();
    boost::filesystem::path blob_path = create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    boost::filesystem::path blob_relative = boost::filesystem::relative(blob_path, location);

    run_offline_compaction("--make_backup");

    // The compacted log directory must keep the blob data.
    EXPECT_TRUE(boost::filesystem::exists(blob_path));

    // The backup directory must also keep its own copy of the blob data.
    std::vector<boost::filesystem::path> backup_dirs = find_backup_dirs();
    ASSERT_EQ(backup_dirs.size(), 1U);
    EXPECT_TRUE(boost::filesystem::exists(backup_dirs[0] / blob_relative));

    remove_backup_dirs();
}

// With --make_backup, the backup destination is generated internally, so it must be
// reported on stdout ("backup-directory: <path>") for the user to locate it. The
// reported path must match the backup directory actually created.
TEST_F(offline_compaction_test, offline_compaction_reports_backup_directory) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key", "value", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    std::string out;
    std::string command = std::string(util_command) + " compaction --force --make_backup " +
        std::string(location) + " 2>&1";
    int rc = invoke(command, out);
    ASSERT_EQ(rc, 0) << "invoke failed: " << out;

    std::vector<boost::filesystem::path> backup_dirs = find_backup_dirs();
    ASSERT_EQ(backup_dirs.size(), 1U);

    // The backup directory must be reported on stdout with its actual path. The path is
    // printed via boost::filesystem::path's operator<<, which quotes it, so build the
    // expected string the same way.
    std::ostringstream expected;
    expected << "backup-directory: " << backup_dirs[0];
    EXPECT_TRUE(out.find(expected.str()) != std::string::npos)
        << "expected \"" << expected.str() << "\" in tglogutil output:\n" << out;

    remove_backup_dirs();
}

// Offline compaction must not silently drop any file it is not explicitly expected to
// consume. The purpose of this test is to detect the case where a future change adds a
// new file to the log directory and compaction is not updated to carry it over: any
// top-level entry present before compaction that is not on the allow-list of consumed
// entries (see is_expected_to_disappear) must still be present afterwards. This is
// exactly the class of bug that lost the blob directory.
TEST_F(offline_compaction_test, offline_compaction_does_not_drop_unexpected_files) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->add_entry(1, "plain_key", "plain_value", {1, 1});
    lc0_->end_session();
    create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    std::set<std::string> before = list_top_level_entries(location);
    // The directory must contain the files whose preservation we care about.
    ASSERT_NE(before.count(std::string(manifest::file_name)), 0U);
    ASSERT_NE(before.count(compaction_catalog::get_catalog_filename()), 0U);
    ASSERT_NE(before.count("blob"), 0U);

    run_offline_compaction();

    std::set<std::string> after = list_top_level_entries(location);
    for (std::string const& name : before) {
        if (is_expected_to_disappear(name)) {
            continue;
        }
        EXPECT_NE(after.count(name), 0U)
            << "unexpected file lost during offline compaction: " << name;
    }
}

// With --make_backup, the backup directory must be a byte-exact copy of the log directory
// as it was right before compaction: same set of files, same contents. This ensures the
// backup is a faithful, fully recoverable image of the pre-compaction state.
TEST_F(offline_compaction_test, offline_compaction_backup_matches_pre_compaction_state) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->add_entry(1, "plain_key", "plain_value", {1, 1});
    lc0_->end_session();
    create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Snapshot the whole log directory right before compaction.
    std::map<std::string, std::string> before = snapshot_tree(location);

    run_offline_compaction("--make_backup");

    std::vector<boost::filesystem::path> backup_dirs = find_backup_dirs();
    ASSERT_EQ(backup_dirs.size(), 1U);

    // The backup must be identical to the pre-compaction directory, file for file.
    std::map<std::string, std::string> backup = snapshot_tree(backup_dirs[0]);
    EXPECT_EQ(backup, before);

    remove_backup_dirs();
}

// When the blob directory contains a broken entry that cannot be copied, the backup-mode
// copy of the blob directory must fail with a clear message and leave the original log
// directory untouched. A dangling symlink is used to force the failure: boost::filesystem
// follows it and fails with ENOENT regardless of the caller's privileges, so this exercises
// the copy-failure path even when the test runs as root (as it does in CI).
TEST_F(offline_compaction_test, offline_compaction_backup_fails_when_blob_directory_copy_fails) {
    gen_datastore();
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "blob_key", "blob_value", {1, 0}, {1001});
    lc0_->end_session();
    boost::filesystem::path blob_path = create_dummy_blob_files(1001);
    datastore_->set_next_blob_id(1002);
    datastore_->switch_epoch(2);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Place a dangling symlink inside the blob directory so that the recursive copy fails
    // when it tries to follow it. This is independent of file permissions, so it also
    // reproduces the failure when running as root.
    boost::filesystem::path dangling = blob_path.parent_path() / "dangling.blob";
    boost::filesystem::create_symlink("/nonexistent/target", dangling);

    std::string out;
    std::string command = std::string(util_command) + " compaction --force --make_backup " +
        std::string(location) + " 2>&1";
    int rc = invoke(command, out);
    EXPECT_NE(rc, 0) << "compaction should fail when a blob entry cannot be copied";
    EXPECT_NE(out.find("failed to copy directory"), std::string::npos)
        << "tglogutil output:\n" << out;

    // The copy failure happens before the directory swap, so the original log directory
    // must be fully intact and no backup directory must have been created.
    EXPECT_TRUE(boost::filesystem::exists(blob_path));
    EXPECT_TRUE(find_backup_dirs().empty());

    remove_work_dirs();
}

}  // namespace limestone::testing
