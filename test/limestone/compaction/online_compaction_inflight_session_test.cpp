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
// The REMAINING bug (until the compaction redesign of design doc 04 lands) is on the
// compaction side: the scan treats that boundary-exceeding snippet as non-durable,
// physically overwrites its marker_begin with marker_invalidated_begin (repair_by_mark)
// and registers the file as a detached pwal, excluding it from every future snapshot. The
// session's epoch is nevertheless reported durable to the upper layer, so the entries
// written by the session are silently lost on the next restart.
//
// These tests reproduce that data loss deterministically and verify the traces the bug
// leaves behind (the invalidated snippet header, the detached-pwal registration and the
// entries missing from the snapshot). The assertions therefore encode the CURRENT BUGGY
// behavior and pass as long as the bug is present; they must be inverted when the bug is
// fixed.
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
    // "pwal_0001.<unixtime>.<epoch>"), or an empty path if none exists.
    [[nodiscard]] boost::filesystem::path find_rotated_pwal(std::string const& prefix) const {
        boost::filesystem::directory_iterator end;
        for (boost::filesystem::directory_iterator it{boost::filesystem::path(location)};
             it != end; ++it) {
            std::string name = it->path().filename().string();
            if (name.rfind(prefix, 0) == 0) {
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
// session-boundary rename. The compaction scan then sees the complete epoch-3 snippet in
// the rotated file, invalidates its header and registers the file as a detached pwal.
TEST_F(online_compaction_inflight_session_test, unflushed_inflight_session_detached_and_lost) {
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

    // BUG TRACE: the rotated pwal holds the complete snippet of epoch 3, but the
    // compaction scan treated it as non-durable and physically overwrote its marker_begin
    // with marker_invalidated_begin.
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated.empty());
    std::vector<log_entry> entries = read_raw_entries(rotated);
    ASSERT_EQ(entries.size(), 3);
    EXPECT_EQ(entries[0].type(), log_entry::entry_type::marker_invalidated_begin);
    EXPECT_EQ(entries[0].epoch_id(), 3);
    EXPECT_EQ(entries[1].type(), log_entry::entry_type::normal_entry);
    EXPECT_TRUE(contains_normal_entry_with_key(entries, "k2"));
    EXPECT_EQ(entries[2].type(), log_entry::entry_type::marker_end);

    // BUG TRACE: the file was registered as a detached pwal, so the durable snippet above
    // is permanently excluded from snapshot input.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated.filename().string()), 1);

    // Consequence (indirect check): the entries of the durable epoch 3 are lost after a
    // restart, although the WAL file still holds them.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_FALSE(contains(kv_list, "k2", "v2"));
    EXPECT_EQ(kv_list.size(), 1);
}

// Write-side variant 2: the in-flight session has written more than the 128KiB stdio
// buffer, so the begin_session marker and a prefix of the entries are already visible on
// disk when the rotation is requested. The rename still happens only at the session
// boundary, so the compaction scan sees the same complete epoch-3 snippet as variant 1 and
// leaves the same traces.
TEST_F(online_compaction_inflight_session_test, partially_flushed_inflight_session_marked_invalid_and_lost) {
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

    // BUG TRACE: the compaction scan has physically rewritten the snippet header of the
    // still-open file. The raw entry sequence shows marker_invalidated_begin instead of
    // marker_begin, while all entries of the session, followed by the regular marker_end,
    // are present in the file.
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated.empty());
    std::vector<log_entry> entries = read_raw_entries(rotated);
    ASSERT_EQ(entries.size(), filler_count + 3);  // snippet header + entries + marker_end
    EXPECT_EQ(entries[0].type(), log_entry::entry_type::marker_invalidated_begin);
    EXPECT_EQ(entries[0].epoch_id(), 3);
    EXPECT_TRUE(contains_normal_entry_with_key(entries, "k2"));
    EXPECT_EQ(entries.back().type(), log_entry::entry_type::marker_end);

    // BUG TRACE: the file was also registered as a detached pwal.
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_detached_pwals().count(rotated.filename().string()), 1);

    // Consequence (indirect check): the entries of the durable epoch 3 are lost after a
    // restart.
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    EXPECT_TRUE(contains(kv_list, "k1", "v1"));
    EXPECT_FALSE(contains(kv_list, "k2", "v2"));
    EXPECT_EQ(kv_list.size(), 1);
}

} // namespace limestone::testing
