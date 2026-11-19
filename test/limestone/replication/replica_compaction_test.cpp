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

class replica_compaction_test : public replica_compaction_test_fixture {
public:
    replica_compaction_test() : replica_compaction_test_fixture("/tmp/replica_compaction_test") {}
};

// レプリカの起動処理は、ワーカースレッドを起動する。ctrl ディレクトリは作らない
TEST_F(replica_compaction_test, startup_starts_the_worker) {
    start_replica(auto_compaction_settings{});

    ASSERT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher());
    EXPECT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher()->launched());
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "ctrl"));
    stop();
}

// 受信 epoch が 0 の間は、WAL の量が閾値を超えていても実行されない。GROUP_COMMIT を受け取ると、受信 epoch を
// 境界にしてローテーションとコンパクションが行われる。境界 epoch を超えるエントリは、キャリーファイルへ送られる
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

    // epoch 3 のエントリは、GROUP_COMMIT(3) を受け取った後の起動で読める
    receive_group_commit(3);
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// GROUP_COMMIT を受け取っていない epoch のエントリは、コンパクションの後も、起動時に無効として扱われる
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

// 起動時の受信 epoch (durable epoch) を境界にして、起動の直後からコンパクションが行われる
TEST_F(replica_compaction_test, compacts_after_restart_with_initial_received_epoch) {
    start_replica(auto_compaction_settings{});
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

// レプリカは ctrl/start_compaction を無視する。ファイルは残り、コンパクションは実行されない
TEST_F(replica_compaction_test, ignores_ctrl_file) {
    auto_compaction_settings settings{};
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

// ローテーションと並行して登録され、セッションを開始したログチャネルのファイルは、その場ではリネームされない。
// リネームはチャネルに委ねられ、ローテーションはセッションの終了を待つ
TEST_F(replica_compaction_test, channel_registered_during_rotation_is_not_renamed_on_the_spot) {
    // 前回の実行で残った、ローテーション前の WAL ファイルを作る
    start_replica(auto_compaction_settings{});
    {
        log_channel& channel = datastore_->create_channel();
        receive_session(channel, 1, "k1", "v1");
        receive_group_commit(1);
    }
    stop();

    // ローテーションが始まった時点で、チャネルを登録してセッションを開始する。このチャネルは、残っていた
    // ファイルと同じ名前のファイルに追記する
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

    // セッションが続いている間は、ファイルはリネームされず、ローテーションは完了しない
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

// ローテーションがセッションの終了を待っている間に shutdown() すると、ローテーションは打ち切られ、正常に終了する。
// コンパクションは行われず、永続化データはそのまま読める
TEST_F(replica_compaction_test, shutdown_while_rotation_is_waiting) {
    start_replica(eager_settings());
    log_channel& channel = datastore_->create_channel();
    receive_session(channel, 1, "k1", "v1");
    // セッションを終了しないので、ローテーションはこのチャネルのリネームを待ち続ける
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

// 形式バージョンの移行に伴うローテーションが要るときは、起動処理が、残っている WAL ファイルをローテーションして
// から、ワーカースレッドを起動する
TEST_F(replica_compaction_test, startup_rotates_wal_files_for_format_version_migration) {
    start_replica(auto_compaction_settings{});
    {
        log_channel& channel = datastore_->create_channel();
        receive_session(channel, 1, "k1", "v1");
        receive_group_commit(1);
    }
    stop();
    ASSERT_TRUE(rotated_wal_files().empty());

    create_replica(auto_compaction_settings{});
    datastore_->get_impl()->set_migration_info(limestone::internal::manifest::migration_info(5, 6));
    datastore_->start_as_replica();

    EXPECT_EQ(rotated_wal_files().size(), 1U);
    ASSERT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher());
    EXPECT_TRUE(datastore_->get_impl()->get_compaction_worker_launcher()->launched());
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// 形式バージョンの移行に伴うローテーションでリネームした WAL ファイルは、その起動の中で、常時コンパクションの
// 入力になる。入力にならないと、WAL の量が閾値を下回らず、測定のたびにコンパクションが繰り返される
TEST_F(replica_compaction_test, wal_files_rotated_for_format_version_migration_are_compacted) {
    start_replica(auto_compaction_settings{});
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
    // リネームしたファイルは、入力になった後、削除される。残っていれば、次の測定でもう一度実行される
    std::this_thread::sleep_for(quiet_period);
    EXPECT_EQ(generation(), 1U);
    EXPECT_TRUE(rotated_wal_files().empty());
    stop();
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}};
    EXPECT_EQ(restart_as_master_and_read(), expected);
}

// 常時コンパクションを行ったレプリカの永続化データから起動したとき、cursor が返すエントリの列が、常時
// コンパクションを行わなかった場合と同一である
TEST_F(replica_compaction_test, entries_are_the_same_with_and_without_auto_compaction) {
    // 同じ受信の列を、指定の設定のレプリカに与えて、その永続化データから読めるエントリを返す。途中で
    // コンパクションが 2 回行われるように、GROUP_COMMIT の後で待つ
    auto run = [this](const auto_compaction_settings& settings, bool expect_compaction) {
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
    const std::vector<std::tuple<std::string, std::string>> without_compaction = run(auto_compaction_settings{}, false);

    // epoch 5 は GROUP_COMMIT を受け取っていないので、どちらでも読めない
    const std::vector<std::tuple<std::string, std::string>> expected{
        {"a", "a2"}, {"b", "b4"}, {"c", "c3"}, {"d", "d4"}};
    EXPECT_EQ(with_compaction, expected);
    EXPECT_EQ(without_compaction, with_compaction);
}

}  // namespace limestone::testing
