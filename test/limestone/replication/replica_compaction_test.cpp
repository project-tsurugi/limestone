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

#include "manifest.h"

namespace limestone::testing {

class replica_compaction_test : public replica_compaction_base_test {
public:
    replica_compaction_test() : replica_compaction_base_test("/tmp/replica_compaction_test") {}
};

// The startup processing of the replica starts the worker thread. It does not create the ctrl directory
TEST_F(replica_compaction_test, startup_starts_the_worker) {
    start_replica(compaction_settings{});

    ASSERT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher());
    EXPECT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher()->launched());
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "ctrl"));
    stop();
}

// While the received epoch is 0, nothing runs even though the amount of the WAL exceeds the threshold. Once a
// GROUP_COMMIT is received, a rotation and a compaction run with the received epoch as the boundary. The entries
// beyond the boundary epoch are sent to the carry file
TEST_F(replica_compaction_test, rotates_and_compacts_at_received_epoch) {
    start_replica(eager_settings());
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    receive_session(channel, 2, "k2", "v2");
    receive_session(channel, 3, "k3", "v3");

    std::this_thread::sleep_for(quiet_period);
    EXPECT_EQ(generation(), 0U);

    receive_group_commit(2);

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location)
                                          / compaction_catalog::get_carry_filename_for_generation(1)));
    EXPECT_EQ(datastore_->epoch_id_switched(), 0U);
    EXPECT_EQ(datastore_->epoch_id_informed(), 0U);

    // The entry of epoch 3 can be read at a startup after GROUP_COMMIT(3) is received
    receive_group_commit(3);
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// The entries of an epoch whose GROUP_COMMIT has not been received are treated as invalid at startup, even
// after a compaction
TEST_F(replica_compaction_test, entries_beyond_received_epoch_stay_nondurable) {
    start_replica(eager_settings());
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    receive_session(channel, 2, "k2", "v2");
    receive_group_commit(1);

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    stop();

    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// A compaction runs right after the startup, with the received epoch at startup (the durable epoch) as the
// boundary
TEST_F(replica_compaction_test, compacts_after_restart_with_initial_received_epoch) {
    start_replica(compaction_settings{});
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    receive_group_commit(1);
    stop();

    start_replica(eager_settings());

    EXPECT_EQ(datastore_->get_impl()->get_received_epoch(), 1U);
    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// The replica ignores ctrl/start_compaction. The file remains and no compaction runs
TEST_F(replica_compaction_test, ignores_ctrl_file) {
    compaction_settings settings{};
    settings.measurement_interval = std::chrono::milliseconds(50);
    start_replica(settings);
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    receive_group_commit(1);
    const boost::filesystem::path ctrl_dir = boost::filesystem::path(location) / "ctrl";
    ASSERT_TRUE(boost::filesystem::create_directory(ctrl_dir));
    {
        boost::filesystem::ofstream stream(ctrl_dir / "start_compaction");
    }

    std::this_thread::sleep_for(quiet_period);

    EXPECT_EQ(generation(), 0U);
    EXPECT_TRUE(boost::filesystem::exists(ctrl_dir / "start_compaction"));
    stop();
}

// The file of a log channel that is registered and begins a session concurrently with a rotation is not renamed
// on the spot. The rename is delegated to the channel, and the rotation waits for the session to end
TEST_F(replica_compaction_test, channel_registered_during_rotation_is_not_renamed_on_the_spot) {
    // Create an unrotated WAL file left by the previous run
    start_replica(compaction_settings{});
    {
        log_channel& channel = datastore_->create_channel();
        receive_session(channel, 1, "k1", "v1");
        receive_group_commit(1);
    }
    stop();

    // When the rotation starts, register a channel and begin a session. This channel appends to the file with
    // the same name as the file left
    create_replica(eager_settings());
    std::atomic<log_channel*> late_channel{nullptr};
    datastore_->on_rotate_log_files_callback = [this, &late_channel]() {
        if (late_channel.load() != nullptr) {
            return;
        }
        log_channel& channel = datastore_->create_channel();
        channel.get_impl()->begin_session_at(2);
        channel.add_entry(1, "k2", "v2", {2, 0});
        late_channel.store(&channel);
    };
    datastore_->start_as_replica();

    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (late_channel.load() == nullptr && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_NE(late_channel.load(), nullptr);

    // While the session continues, the file is not renamed and the rotation does not complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(rotated_wal_files().empty());
    EXPECT_EQ(generation(), 0U);

    late_channel.load()->get_impl()->end_session_at(2);

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    receive_group_commit(2);
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}, {"k2", "v2"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// shutdown() while the rotation is waiting for a session to end gives up the rotation and completes normally.
// No compaction is performed, and the persistent data can be read as it is
TEST_F(replica_compaction_test, shutdown_while_rotation_is_waiting) {
    start_replica(eager_settings());
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    // The session is not ended, so the rotation keeps waiting for the rename by this channel
    channel.get_impl()->begin_session_at(2);
    channel.add_entry(1, "k2", "v2", {2, 0});
    std::atomic<bool> rotation_started{false};
    datastore_->on_rotate_log_files_callback = [&rotation_started]() { rotation_started.store(true); };

    receive_group_commit(1);
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (! rotation_started.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(rotation_started.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    datastore_->shutdown();

    EXPECT_EQ(generation(), 0U);
    EXPECT_TRUE(rotated_wal_files().empty());
    datastore_ = nullptr;
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// When the migration of the format version requires a rotation, the startup processing rotates the WAL files
// left and then starts the worker thread
TEST_F(replica_compaction_test, startup_rotates_wal_files_for_format_version_migration) {
    start_replica(compaction_settings{});
    {
        log_channel& channel = datastore_->create_channel();
        receive_session(channel, 1, "k1", "v1");
        receive_group_commit(1);
    }
    stop();
    ASSERT_TRUE(rotated_wal_files().empty());

    create_replica(compaction_settings{});
    datastore_->get_impl()->set_migration_info(limestone::internal::manifest::migration_info(5, 6));
    datastore_->start_as_replica();

    EXPECT_EQ(rotated_wal_files().size(), 1U);
    ASSERT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher());
    EXPECT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher()->launched());
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// The WAL files renamed by the rotation for the migration of the format version become, within that run, the
// input of the auto compaction. If they did not, the amount of the WAL would never fall below the threshold and
// a compaction would be repeated at every measurement
TEST_F(replica_compaction_test, wal_files_rotated_for_format_version_migration_are_compacted) {
    start_replica(compaction_settings{});
    {
        log_channel& channel = datastore_->create_channel();
        receive_session(channel, 1, "k1", "v1");
        receive_group_commit(1);
    }
    stop();

    create_replica(eager_settings());
    datastore_->get_impl()->set_migration_info(limestone::internal::manifest::migration_info(5, 6));
    datastore_->start_as_replica();

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    // The renamed file is removed after it becomes the input. If it were left, a compaction would run again at
    // the next measurement
    std::this_thread::sleep_for(quiet_period);
    EXPECT_EQ(generation(), 1U);
    EXPECT_TRUE(rotated_wal_files().empty());
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// At a startup from the persistent data of a replica that ran the auto compaction, the sequence of the entries
// the cursor returns is the same as when the auto compaction did not run
TEST_F(replica_compaction_test, entries_are_the_same_with_and_without_auto_compaction) {
    // Gives the same sequence of receptions to a replica with the given settings, and returns the entries that
    // can be read from its persistent data. It waits after each GROUP_COMMIT so that a compaction runs twice
    auto run = [this](const compaction_settings& settings, bool expect_compaction) {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directory(location);
        start_replica(settings);
        log_channel& channel0 = datastore_->create_channel();
        log_channel& channel1 = datastore_->create_channel();
        receive_session(channel0, 1, "a", "a1");
        receive_session(channel1, 1, "b", "b1");
        receive_session(channel0, 2, "a", "a2");
        receive_session(channel1, 3, "c", "c3");
        receive_group_commit(2);
        if (expect_compaction) {
            EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
        }
        receive_session(channel0, 4, "b", "b4");
        receive_session(channel1, 4, "d", "d4");
        receive_session(channel0, 5, "e", "e5");
        receive_group_commit(4);
        if (expect_compaction) {
            EXPECT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
        } else {
            EXPECT_EQ(generation(), 0U);
        }
        stop();
        std::vector<std::tuple<std::string, std::string>> entries = restart_as_master_and_read();
        stop();
        return entries;
    };

    const std::vector<std::tuple<std::string, std::string>> with_compaction = run(eager_settings(), true);
    const std::vector<std::tuple<std::string, std::string>> without_compaction = run(compaction_settings{}, false);

    // The GROUP_COMMIT of epoch 5 has not been received, so its entry cannot be read in either case
    const std::vector<std::tuple<std::string, std::string>> expected{
        {"a", "a2"}, {"b", "b4"}, {"c", "c3"}, {"d", "d4"}};
    EXPECT_EQ(with_compaction, expected);
    EXPECT_EQ(without_compaction, with_compaction);
}

}  // namespace limestone::testing
