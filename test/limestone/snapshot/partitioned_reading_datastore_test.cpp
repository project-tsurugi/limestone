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
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/snapshot.h>

#include "limestone/compaction/compaction_test_fixture.h"
#include "datastore_impl.h"
#include "locator/locator_format.h"

namespace limestone::testing {

/**
 * The overall test of get_partitioned_cursors() at the datastore level. Reads the snapshot file and the compacted WAL
 * file (and their locator files) actually made from the entries written to the datastore through the public API
 * get_partitioned_cursors(), and checks that the output matches that of get_cursor().
 */
class partitioned_reading_datastore_test : public compaction_test {
public:
    partitioned_reading_datastore_test() : compaction_test("/tmp/partitioned_reading_datastore_test") {}

    /// one row returned by a cursor (storage, key, value)
    using row = std::tuple<std::uint64_t, std::string, std::string>;
    using rows = std::vector<row>;

    // starts the datastore with a small record interval (64 bytes, R = 1). ready() makes the snapshot file and its
    // locator file
    void start_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_locator_settings_for_test({64, 1});
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
        datastore_->wait_for_blob_file_garbace_collector();
    }

    void restart_datastore() {
        datastore_->shutdown();
        datastore_ = nullptr;
        start_datastore();
    }

    static std::string key_of(int i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%04d", i);
        return buf;
    }

    // writes the entries of the keys in one session, and advances the epoch
    void write_entries(int first, int last, const std::string& value, epoch_id_type epoch) {
        lc0_->begin_session();
        for (int i = first; i < last; ++i) {
            lc0_->add_entry(1, key_of(i), value + key_of(i), {epoch, 0});
        }
        lc0_->end_session();
        datastore_->switch_epoch(epoch + 1);
    }

    void remove_entries(int first, int last, int step, epoch_id_type epoch) {
        lc0_->begin_session();
        for (int i = first; i < last; i += step) {
            lc0_->remove_entry(1, key_of(i), {epoch, 0});
        }
        lc0_->end_session();
        datastore_->switch_epoch(epoch + 1);
    }

    static rows read_all(api::cursor& cursor) {
        rows result{};
        while (cursor.next()) {
            std::string key{};
            std::string value{};
            cursor.key(key);
            cursor.value(value);
            result.emplace_back(cursor.storage(), key, value);
        }
        return result;
    }

    // compares the output of get_cursor() with the outputs of n cursors together. The output of each cursor must be in
    // ascending order
    void check_partitioned_matches_full(const std::vector<std::size_t>& ns, std::size_t expected_count) {
        const rows expected = read_all(*datastore_->get_snapshot()->get_cursor());
        ASSERT_EQ(expected.size(), expected_count);
        ASSERT_TRUE(std::is_sorted(expected.begin(), expected.end()));
        for (std::size_t n : ns) {
            std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
            std::vector<std::unique_ptr<api::cursor>> cursors = snapshot->get_partitioned_cursors(n);
            ASSERT_EQ(cursors.size(), n);
            rows all{};
            for (std::unique_ptr<api::cursor>& cursor : cursors) {
                rows r = read_all(*cursor);
                EXPECT_TRUE(std::is_sorted(r.begin(), r.end())) << "n = " << n;
                all.insert(all.end(), r.begin(), r.end());
            }
            std::sort(all.begin(), all.end());
            EXPECT_EQ(all, expected) << "n = " << n;
        }
    }
};

// no compacted WAL file, only the snapshot file
TEST_F(partitioned_reading_datastore_test, snapshot_only) {
    start_datastore();
    datastore_->switch_epoch(1);
    write_entries(0, 300, "a", 1);
    write_entries(100, 200, "b", 2);  // overwrite
    remove_entries(0, 300, 7, 3);
    restart_datastore();
    EXPECT_TRUE(compacted_filename().empty());
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "data" / "snapshot.loc"));
    // of the 300 entries, the multiples of 7 (43 entries) are removed
    check_partitioned_matches_full({1, 2, 3, 8}, 300 - 43);
}

// with a compacted WAL file: the entries written after the compaction are in the snapshot file, and those written
// before are in the compacted WAL file. Includes overwrites and removals of the same keys
TEST_F(partitioned_reading_datastore_test, with_compacted_file) {
    start_datastore();
    datastore_->switch_epoch(1);
    write_entries(0, 400, "a", 1);
    write_entries(50, 150, "b", 2);
    run_compact_with_epoch_switch(4);
    ASSERT_FALSE(compacted_filename().empty());
    write_entries(300, 500, "c", 5);  // 100 overwrites and 100 additions
    remove_entries(0, 100, 5, 6);      // removes 20 entries of the compacted side
    restart_datastore();
    ASSERT_FALSE(compacted_filename().empty());
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location)
                                          / internal::locator_format::locator_file_name(compacted_filename())));
    check_partitioned_matches_full({1, 2, 3, 8}, 500 - 20);
}

}  // namespace limestone::testing
