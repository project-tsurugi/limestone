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

#include <algorithm>
#include <cctype>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "compaction_test_fixture.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

// Tests for the race between online compaction and a log-channel session that is in
// flight when rotate_log_files() is requested.
//
// With the WAL rotation mechanism (issue #139, design doc 03) in place, the rotation no
// longer renames a file that an in-flight session keeps open: it waits until the session
// reaches end_session(), and the rename happens at the session boundary. The rotated file
// therefore always holds the session's complete snippet, whose epoch is necessarily larger
// than the rotation boundary epoch.
//
// With the carry output of the compaction redesign in place, the compaction preserves
// that boundary-exceeding snippet: the scan skips it without touching it (no
// invalidation), and a separate pass copies its raw bytes into the carry file, which is
// recorded in the catalog and fed to the snapshot input at the next startup. The source
// file is registered as a detached pwal only after its content is preserved this way.
//
// These tests verify that fix deterministically: the snippet header stays untouched, the
// carry file holds the snippet, and the entries of the session survive a restart.
class online_compaction_inflight_session_test : public compaction_test {
public:
    online_compaction_inflight_session_test()
        : compaction_test("/tmp/online_compaction_inflight_session_test") {}

protected:
    // Runs compact_with_online() on a background thread and executes in_window on this
    // thread while the compaction thread is stopped inside rotate_log_files(), after it
    // has read the rotation epoch but before it waits for epoch_id_informed_ and selects
    // the rotation targets. switch_epoch(next_epoch) is issued first so that the wait for
    // epoch_id_informed_ can complete; a session started by in_window therefore belongs
    // to next_epoch, which is larger than the rotation epoch.
    //
    // The rotation waits until the in-flight session reaches end_session(), so after the
    // compaction thread is released, after_release must complete the session (typically by
    // calling end_session()); this helper then waits for the compaction to finish.
    void run_compact_with_inflight_session(epoch_id_type next_epoch,
                                           std::function<void()> const& in_window,
                                           std::function<void()> const& after_release) {
        auto* test_datastore = dynamic_cast<datastore_test*>(datastore_.get());
        ASSERT_NE(test_datastore, nullptr);

        std::mutex mtx;
        std::condition_variable cv;
        bool reached = false;
        bool released = false;

        test_datastore->on_rotate_log_files_callback = [&]() {
            {
                std::unique_lock<std::mutex> lock(mtx);
                reached = true;
                cv.notify_all();
                cv.wait(lock, [&] { return released; });
            }
        };

        auto future = std::async(std::launch::async, [this] { datastore_->compact_with_online(); });
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return reached; });
        }

        datastore_->switch_epoch(next_epoch);
        in_window();

        {
            std::lock_guard<std::mutex> lock(mtx);
            released = true;
            cv.notify_all();
        }
        after_release();
        future.get();
        test_datastore->on_rotate_log_files_callback = nullptr;
    }

    // Returns the rotated file of the given pwal (e.g. "pwal_0001." matches
    // "pwal_0001.<unixtime>.<epoch>"), or an empty path if none exists. The character
    // after the prefix must be a digit, so that "pwal_0000." never matches the
    // compaction outputs ("pwal_0000.compacted.<gen>" / "pwal_0000.carry.<gen>").
    [[nodiscard]] boost::filesystem::path find_rotated_pwal(std::string const& prefix) const {
        boost::filesystem::directory_iterator end;
        for (boost::filesystem::directory_iterator it{boost::filesystem::path(location)};
             it != end; ++it) {
            std::string name = it->path().filename().string();
            if (name.rfind(prefix, 0) == 0 && name.size() > prefix.size() &&
                std::isdigit(static_cast<unsigned char>(name[prefix.size()])) != 0) {
                return it->path();
            }
        }
        return {};
    }

    // Reads all physical log entries of the file, including the epoch snippet markers.
    // Unlike dblog_scan-based helpers, this does not interpret snippet validity, so it
    // exposes a marker_invalidated_begin written by repair_by_mark as-is and also returns
    // the entries of an invalidated snippet.
    static std::vector<log_entry> read_raw_entries(boost::filesystem::path const& path) {
        std::vector<log_entry> entries;
        std::ifstream in;
        in.open(path.string(), std::ios::in | std::ios::binary);
        log_entry e;
        while (e.read(in)) {
            entries.push_back(e);
        }
        return entries;
    }

    static bool contains_normal_entry_with_key(std::vector<log_entry> const& entries,
                                               std::string const& key) {
        return std::any_of(entries.begin(), entries.end(), [&key](log_entry const& e) {
            if (e.type() != log_entry::entry_type::normal_entry) {
                return false;
            }
            std::string k;
            e.key(k);
            return k == key;
        });
    }

    static bool contains(std::vector<std::pair<std::string, std::string>> const& kv_list,
                         std::string const& key, std::string const& value) {
        return std::find(kv_list.begin(), kv_list.end(), std::make_pair(key, value))
            != kv_list.end();
    }
};

// Write-side variant 1: the in-flight session has written so little that everything,
// including the begin_session marker, still sits in the stdio buffer when the rotation is
// requested; the complete snippet reaches the disk only at end_session(), just before the
// session-boundary rename. The compaction scan reads the rotated file with the rotation
// boundary epoch, skips the complete epoch-3 snippet without touching it, and the carry
// pass preserves the snippet before the file is registered as a detached pwal.
TEST_F(online_compaction_inflight_session_test, unflushed_inflight_session_preserved_via_carry) {
    gen_datastore();
    datastore_->switch_epoch(1);

    // Make epoch 1 durable with a completed session on another channel.
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    // Start a session on lc1 inside the rotation window; it belongs to epoch 3, which is
    // larger than the rotation epoch 2. The rotation waits for this session, so the
    // session is completed (end_session) after the compaction thread is released; the
    // rename then happens at the session boundary and the compaction proceeds.
    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
        },
        [this] { lc1_->end_session(); });

    // Epoch 3 is reported durable to the upper layer.
    datastore_->switch_epoch(4);
    EXPECT_GE(datastore_->last_epoch(), 3);

    // The rotated pwal holds the complete snippet of epoch 3, untouched: the compaction
    // scan no longer invalidates snippets beyond the boundary (its marker_begin stays).
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated.empty());
    std::vector<log_entry> entries = read_raw_entries(rotated);
    ASSERT_EQ(entries.size(), 3);
    EXPECT_EQ(entries[0].type(), log_entry::entry_type::marker_begin);
    EXPECT_EQ(entries[0].epoch_id(), 3);
    EXPECT_EQ(entries[1].type(), log_entry::entry_type::normal_entry);
    EXPECT_TRUE(contains_normal_entry_with_key(entries, "k2"));
    EXPECT_EQ(entries[2].type(), log_entry::entry_type::marker_end);

    // The file is registered as a detached pwal, and the boundary-exceeding snippet is
    // preserved in the carry file recorded by the catalog.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated.filename().string()), 1);
    ASSERT_TRUE(catalog.get_carry_file().has_value());
    boost::filesystem::path carry = boost::filesystem::path(location) / catalog.get_carry_file().value();
    ASSERT_TRUE(boost::filesystem::exists(carry));
    // The carry file does not enter detached_pwals: it has to be selected as an input
    // of the next compaction and of the scan at startup.
    EXPECT_EQ(catalog.get_detached_pwals().count(catalog.get_carry_file().value()), 0);
    std::vector<log_entry> carry_entries = read_raw_entries(carry);
    ASSERT_FALSE(carry_entries.empty());
    EXPECT_EQ(carry_entries[0].type(), log_entry::entry_type::marker_begin);
    EXPECT_EQ(carry_entries[0].epoch_id(), 3);
    EXPECT_TRUE(contains_normal_entry_with_key(carry_entries, "k2"));

    // The entries of the durable epoch 3 survive a restart: the carry file is part of
    // the snapshot input.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_TRUE(contains(kv_list, "k2", "v2"));
    EXPECT_EQ(kv_list.size(), 2);
}

// Write-side variant 2: the in-flight session has written more than the 128KiB stdio
// buffer, so the begin_session marker and a prefix of the entries are already visible on
// disk when the rotation is requested. The rename still happens only at the session
// boundary, so the compaction scan sees the same complete epoch-3 snippet as variant 1 and
// leaves the same traces.
TEST_F(online_compaction_inflight_session_test, partially_flushed_inflight_session_preserved_via_carry) {
    constexpr int filler_count = 256;  // 256 entries x 1KiB values > 128KiB stdio buffer

    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    std::string const large_value(1024, 'x');
    run_compact_with_inflight_session(3,
        [&] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
            for (int i = 0; i < filler_count; i++) {
                std::ostringstream key;
                key << "fill_" << std::setw(4) << std::setfill('0') << i;
                lc1_->add_entry(1, key.str(), large_value, {3, static_cast<std::uint64_t>(i) + 1});
            }
        },
        [this] { lc1_->end_session(); });

    // Epoch 3 is reported durable to the upper layer.
    datastore_->switch_epoch(4);
    EXPECT_GE(datastore_->last_epoch(), 3);

    // The snippet is untouched: marker_begin stays as written, and all entries of the
    // session, followed by the regular marker_end, are present in the file.
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated.empty());
    std::vector<log_entry> entries = read_raw_entries(rotated);
    ASSERT_EQ(entries.size(), filler_count + 3);  // snippet header + entries + marker_end
    EXPECT_EQ(entries[0].type(), log_entry::entry_type::marker_begin);
    EXPECT_EQ(entries[0].epoch_id(), 3);
    EXPECT_TRUE(contains_normal_entry_with_key(entries, "k2"));
    EXPECT_EQ(entries.back().type(), log_entry::entry_type::marker_end);

    // The file is registered as a detached pwal, and the snippet is preserved in the
    // carry file.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated.filename().string()), 1);
    ASSERT_TRUE(catalog.get_carry_file().has_value());
    ASSERT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / catalog.get_carry_file().value()));

    // All entries of the durable epoch 3 survive a restart.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_TRUE(contains(kv_list, "k2", "v2"));
    EXPECT_EQ(kv_list.size(), 2 + filler_count);
}

// The carry file is absorbed by the next compaction: once the rotation boundary has
// advanced past the carried epoch, its snippets are merged into the new compacted file
// and no new carry is produced. The old carry file is deleted with the old generation.
TEST_F(online_compaction_inflight_session_test, carry_is_absorbed_by_the_next_compaction) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
        },
        [this] { lc1_->end_session(); });
    datastore_->switch_epoch(4);

    compaction_catalog catalog_after_first = compaction_catalog::from_catalog_file(location);
    ASSERT_TRUE(catalog_after_first.get_carry_file().has_value());
    std::string first_carry_name = catalog_after_first.get_carry_file().value();
    std::uint64_t first_generation = catalog_after_first.get_generation();

    // Write more data and compact again; the boundary has advanced past epoch 3, so the
    // carried snippet is merged into the new compacted file.
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v3", {4, 0});
    lc0_->end_session();
    datastore_->switch_epoch(5);
    run_compact_with_epoch_switch(6);

    compaction_catalog catalog_after_second = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog_after_second.get_generation(), first_generation + 1);
    EXPECT_FALSE(catalog_after_second.get_carry_file().has_value());
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / first_carry_name));

    // All entries, including the carried one, survive a restart.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_TRUE(contains(kv_list, "k2", "v2"));
    EXPECT_TRUE(contains(kv_list, "k3", "v3"));
    EXPECT_EQ(kv_list.size(), 3);
}

// When the snippets beyond the boundary are spread over several channels, the carry
// file is a single file concatenating the snippets of each source file. This verifies
// end to end that the scan at startup reads such a concatenated carry file and restores
// the entries of every channel.
TEST_F(online_compaction_inflight_session_test, carry_from_multiple_channels_is_read_at_startup) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    // Open a session on both lc1 and lc2 inside the rotation window. Both belong to
    // epoch 3, so a snippet beyond the boundary (epoch 2) arises in two files.
    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
            lc2_->begin_session();
            lc2_->add_entry(1, "k3", "v3", {3, 1});
        },
        [this] {
            lc1_->end_session();
            lc2_->end_session();
        });

    datastore_->switch_epoch(4);
    EXPECT_GE(datastore_->last_epoch(), 3);

    // The carry file is a single file holding the snippets of both channels, so it has
    // two snippet headers.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_TRUE(catalog.get_carry_file().has_value());
    boost::filesystem::path carry = boost::filesystem::path(location) / catalog.get_carry_file().value();
    ASSERT_TRUE(boost::filesystem::exists(carry));
    std::vector<log_entry> carry_entries = read_raw_entries(carry);
    EXPECT_TRUE(contains_normal_entry_with_key(carry_entries, "k2"));
    EXPECT_TRUE(contains_normal_entry_with_key(carry_entries, "k3"));
    int marker_begin_count = 0;
    for (log_entry const& e : carry_entries) {
        if (e.type() == log_entry::entry_type::marker_begin) {
            EXPECT_EQ(e.epoch_id(), 3);
            marker_begin_count++;
        }
    }
    EXPECT_EQ(marker_begin_count, 2);

    // The source files of both channels are registered as detached pwals.
    boost::filesystem::path rotated1 = find_rotated_pwal("pwal_0001.");
    boost::filesystem::path rotated2 = find_rotated_pwal("pwal_0002.");
    ASSERT_FALSE(rotated1.empty());
    ASSERT_FALSE(rotated2.empty());
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated1.filename().string()), 1);
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated2.filename().string()), 1);

    // The scan at startup reads the concatenated carry file and the entries of both
    // channels are restored.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_TRUE(contains(kv_list, "k2", "v2"));
    EXPECT_TRUE(contains(kv_list, "k3", "v3"));
    EXPECT_EQ(kv_list.size(), 3);
}

// Immutability of rotated files: the content of a compaction input file is
// byte-for-byte identical right after the rename and after the compaction. Both a file
// at or below the boundary (pass 1 only) and a file holding a snippet beyond the
// boundary (pass 1 plus the carry re-read of pass 2) are covered.
TEST_F(online_compaction_inflight_session_test, input_files_are_unchanged_by_compaction) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    // The file at or below the boundary: a rename does not change the content, so the
    // pre-rotation content serves as the reference.
    std::string pwal0_before = read_file_bytes(boost::filesystem::path(location) / "pwal_0000");

    // The file beyond the boundary: end_session performs the rename at the session
    // boundary, so the content right after end_session (= right after the rename) is
    // captured as the reference.
    std::string pwal1_after_rename;
    boost::filesystem::path rotated_at_rename;
    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
        },
        [&] {
            lc1_->end_session();
            rotated_at_rename = find_rotated_pwal("pwal_0001.");
            if (!rotated_at_rename.empty()) {
                pwal1_after_rename = read_file_bytes(rotated_at_rename);
            }
        });
    // An ASSERT inside the lambda would only leave the lambda, so the check is done in the test body.
    ASSERT_FALSE(rotated_at_rename.empty());
    datastore_->switch_epoch(4);

    // Both input files remain unchanged.
    boost::filesystem::path rotated0 = find_rotated_pwal("pwal_0000.");
    boost::filesystem::path rotated1 = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated0.empty());
    ASSERT_FALSE(rotated1.empty());
    EXPECT_EQ(read_file_bytes(rotated0), pwal0_before);
    EXPECT_EQ(read_file_bytes(rotated1), pwal1_after_rename);

    // The carry file is a verbatim copy of the snippets beyond the boundary (this file
    // is entirely beyond the boundary, so the whole content matches).
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_TRUE(catalog.get_carry_file().has_value());
    EXPECT_EQ(read_file_bytes(boost::filesystem::path(location) / catalog.get_carry_file().value()),
              pwal1_after_rename);
}

// Consistency with the write-version reset: an entry in the carry file keeps its real
// write version, so on the merge of the next compaction it wins over the same key of
// the baseline, whose write version has been reset to zero.
TEST_F(online_compaction_inflight_session_test, carried_entry_wins_over_reset_baseline_on_merge) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    run_compact_with_epoch_switch(3);  // generation 1: k1=v1 (write version reset to 0)

    // Update the same key in an in-flight session, so that it is saved into the carry
    // file with its real write version {4,0}.
    run_compact_with_inflight_session(4,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k1", "v2", {4, 0});
        },
        [this] { lc1_->end_session(); });
    datastore_->switch_epoch(5);
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        ASSERT_TRUE(catalog.get_carry_file().has_value());
    }

    // The next compaction absorbs the carry. On the merge of the baseline k1=v1 (WV 0)
    // and the carried k1=v2 (real WV), the real write version has to win.
    run_compact_with_epoch_switch(6);
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        EXPECT_FALSE(catalog.get_carry_file().has_value());
    }

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 1);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v2");
}

// Startup invalidation of the carry: when the epoch of a carried snippet (E') is not
// durable at a restart, the snippet is invalidated by the startup processing and does
// not appear in the snapshot. (A durable E' is treated as ordinary WAL entries; that
// side is covered by the other tests.)
TEST_F(online_compaction_inflight_session_test, non_durable_carry_snippet_is_invalidated_at_startup) {
    boost::filesystem::path crash_copy{std::string(location) + "_crash"};
    boost::filesystem::remove_all(crash_copy);

    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
        },
        [this] { lc1_->end_session(); });

    // Without making epoch 3 durable (no switch_epoch(4)), copy the log directory at
    // this point as the crash state. The durable epoch stays at 2.
    copy_dir_recursive(boost::filesystem::path(location), crash_copy);
    datastore_->shutdown();
    datastore_ = nullptr;
    boost::filesystem::remove_all(boost::filesystem::path(location));
    copy_dir_recursive(crash_copy, boost::filesystem::path(location));
    boost::filesystem::remove_all(crash_copy);

    gen_datastore();

    // The epoch-3 snippet in the carry file is invalidated because it is not durable.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    ASSERT_TRUE(catalog.get_carry_file().has_value());
    boost::filesystem::path carry = boost::filesystem::path(location) / catalog.get_carry_file().value();
    ASSERT_TRUE(boost::filesystem::exists(carry));
    std::vector<log_entry> carry_entries = read_raw_entries(carry);
    ASSERT_FALSE(carry_entries.empty());
    EXPECT_EQ(carry_entries[0].type(), log_entry::entry_type::marker_invalidated_begin);

    // k2 does not appear in the snapshot; only the durable k1 is readable.
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

// Offline compaction on a directory holding a durable carry file: the carry is
// absorbed as an input (the boundary is the durable epoch, so nothing beyond the
// boundary is carried over), and the catalog records the new generation with no carry.
TEST_F(online_compaction_inflight_session_test, offline_compaction_absorbs_durable_carry) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(2);

    run_compact_with_inflight_session(3,
        [this] {
            lc1_->begin_session();
            lc1_->add_entry(1, "k2", "v2", {3, 0});
        },
        [this] { lc1_->end_session(); });
    datastore_->switch_epoch(4);  // make epoch 3 durable

    compaction_catalog catalog_before = compaction_catalog::from_catalog_file(location);
    ASSERT_TRUE(catalog_before.get_carry_file().has_value());
    std::string carry_name = catalog_before.get_carry_file().value();
    std::uint64_t generation_before = catalog_before.get_generation();

    datastore_->shutdown();
    datastore_ = nullptr;

    run_offline_compaction();

    // The carry is absorbed and gone; the catalog holds the new generation with no carry.
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / carry_name));
    {
        compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
        EXPECT_EQ(catalog.get_generation(), generation_before + 1);
        EXPECT_FALSE(catalog.get_carry_file().has_value());
    }

    // All data, including the carried entry, is readable.
    gen_datastore();  // restart_datastore_and_read_snapshot needs a live datastore
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_TRUE(contains(kv_list, "k2", "v2"));
    EXPECT_EQ(kv_list.size(), 2);
}

} // namespace limestone::testing
