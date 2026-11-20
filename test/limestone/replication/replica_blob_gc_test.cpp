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
#include "replica_compaction_test_fixture.h"

namespace limestone::testing {

using limestone::api::blob_id_type;

/**
 * Tests of the BLOB file GC in a compaction on the replica. The boundary of the GC is decided by the compaction
 * boundary received from the master (the boundary epoch of the master E_m and the BLOB ID boundary G_m).
 */
class replica_blob_gc_test : public replica_compaction_base_test {
public:
    replica_blob_gc_test() : replica_compaction_base_test("/tmp/replica_blob_gc_test") {}

    // Creates a BLOB file replicated from the master
    [[nodiscard]] boost::filesystem::path create_blob_file(blob_id_type blob_id) const {
        const boost::filesystem::path path = datastore_->get_impl()->resolve_blob_path(blob_id);
        boost::filesystem::create_directories(path.parent_path());
        boost::filesystem::ofstream stream(path, std::ios::binary);
        stream << "dummy_blob_data";
        return path;
    }

    // Imitates the reception of the logs of one session with a BLOB from the master
    static void receive_session_with_blob(log_channel& channel, epoch_id_type epoch, const std::string& key,
                                          const std::string& value, blob_id_type blob_id) {
        channel.get_impl()->begin_session_at(epoch);
        channel.add_entry(1, key, value, {epoch, 0}, {blob_id});
        channel.get_impl()->end_session_at(epoch);
    }

    // Receives k1 (BLOB 1001) and k2 (BLOB 1002) at epoch 1, and k1 (BLOB 2001) at epoch 2. BLOB 1001 is no
    // longer referenced by any entry. BLOB 3001 is referenced by no entry
    void receive_entries_with_blobs() {
        log_channel& channel = datastore_->create_channel();
        path1001_ = create_blob_file(1001);
        path1002_ = create_blob_file(1002);
        path2001_ = create_blob_file(2001);
        path3001_ = create_blob_file(3001);
        receive_session_with_blob(channel, 1, "k1", "v1", 1001);
        receive_session_with_blob(channel, 1, "k2", "v2", 1002);
        receive_session_with_blob(channel, 2, "k1", "v1-2", 2001);
    }

    // Makes a compaction run with epoch 2 as the boundary
    void compact_at_epoch_2() {
        receive_group_commit(2);
        ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    }

    [[nodiscard]] static bool wait_until_removed(const boost::filesystem::path& path,
                                                 std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (! boost::filesystem::exists(path)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return ! boost::filesystem::exists(path);
    }

    // Checks that the GC did not run. The GC runs on other threads, so wait for a while and stop before checking
    void expect_no_blob_file_removed() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // If the GC has started by mistake, let it finish the removal before checking
        datastore_->wait_for_blob_file_garbace_collector();
        stop();
        EXPECT_TRUE(boost::filesystem::exists(path1001_));
        EXPECT_TRUE(boost::filesystem::exists(path1002_));
        EXPECT_TRUE(boost::filesystem::exists(path2001_));
        EXPECT_TRUE(boost::filesystem::exists(path3001_));
    }

    // Checks that the GC with G_m = 3000 ran. Only BLOB 1001, which is below G_m and not referenced, is removed
    void expect_unreferenced_blob_below_boundary_removed() {
        EXPECT_TRUE(wait_until_removed(path1001_, std::chrono::seconds(10)));
        stop();
        EXPECT_FALSE(boost::filesystem::exists(path1001_));
        EXPECT_TRUE(boost::filesystem::exists(path1002_));
        EXPECT_TRUE(boost::filesystem::exists(path2001_));
        EXPECT_TRUE(boost::filesystem::exists(path3001_));
    }

    boost::filesystem::path path1001_{};
    boost::filesystem::path path1002_{};
    boost::filesystem::path path2001_{};
    boost::filesystem::path path3001_{};
};

// The GC runs when E_m = E_r. Only the BLOB files below G_m and not referenced are removed
TEST_F(replica_blob_gc_test, runs_when_master_boundary_epoch_equals_boundary_epoch) {
    start_replica(eager_settings());
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({2, 3000});

    compact_at_epoch_2();

    expect_unreferenced_blob_below_boundary_removed();
}

// The GC runs when E_m < E_r
TEST_F(replica_blob_gc_test, runs_when_master_boundary_epoch_is_below_boundary_epoch) {
    start_replica(eager_settings());
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({1, 3000});

    compact_at_epoch_2();

    expect_unreferenced_blob_below_boundary_removed();
}

// The BLOB files at or above G_m are not removed even if they are not referenced. If no BLOB file is below G_m
// and unreferenced, nothing is removed
TEST_F(replica_blob_gc_test, keeps_blob_files_at_or_above_blob_id_boundary) {
    start_replica(eager_settings());
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({2, 1001});

    compact_at_epoch_2();

    expect_no_blob_file_removed();
}

// The GC does not run when E_m is greater than E_r
TEST_F(replica_blob_gc_test, does_not_run_when_master_boundary_epoch_is_beyond_boundary_epoch) {
    start_replica(eager_settings());
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({3, 3000});

    compact_at_epoch_2();

    expect_no_blob_file_removed();
}

// The GC does not run when no compaction boundary has been received from the master
TEST_F(replica_blob_gc_test, does_not_run_without_received_boundary) {
    start_replica(eager_settings());
    receive_entries_with_blobs();

    compact_at_epoch_2();

    expect_no_blob_file_removed();
}

// The GC does not run when the setting disables it
TEST_F(replica_blob_gc_test, does_not_run_when_disabled) {
    compaction_settings settings = eager_settings();
    settings.blob_gc = false;
    start_replica(settings);
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({2, 3000});

    compact_at_epoch_2();

    expect_no_blob_file_removed();
}

// At a startup from the persistent data after the GC, the remaining entries can be read
TEST_F(replica_blob_gc_test, data_is_readable_after_gc) {
    start_replica(eager_settings());
    receive_entries_with_blobs();
    datastore_->get_impl()->set_received_compaction_boundary({2, 3000});
    compact_at_epoch_2();
    expect_unreferenced_blob_below_boundary_removed();

    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1-2"}, {"k2", "v2"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

}  // namespace limestone::testing
