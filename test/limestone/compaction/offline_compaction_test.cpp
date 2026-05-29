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
    // Use a dedicated directory so this suite does not collide with compaction_test,
    // which shares the same fixture base; ctest runs the two suites in parallel.
    offline_compaction_test() : compaction_test("/tmp/offline_compaction_test") {}

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
    void run_offline_compaction() {
        std::string out;
        std::string command = std::string(util_command) + " compaction --force " + std::string(location) + " 2>&1";
        int rc = invoke(command, out);
        ASSERT_EQ(rc, 0) << "invoke failed: " << out;
        ASSERT_TRUE(out.find("compaction was successfully completed: ") != std::string::npos)
            << "tglogutil output:\n" << out;
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

}  // namespace limestone::testing
