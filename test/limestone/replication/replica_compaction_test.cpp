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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "auto_compaction_settings.h"
#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "internal.h"
#include "locator/locator_format.h"
#include "log_channel_impl.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::api::log_channel;
using limestone::internal::auto_compaction_settings;
using limestone::internal::compaction_catalog;

/**
 * レプリカでのローテーションとコンパクションのテスト。master からの受信は、ネットワークを使わずに、受信の
 * ハンドラと同じ呼び出し (セッションの開始と終了、GROUP_COMMIT の epoch の永続化と受信 epoch の更新) で模擬する。
 */
class replica_compaction_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/replica_compaction_test";

    // 「実行されない」ことを確かめるときに待つ時間。ワーカースレッドのループの周期 (1 秒) より長くする
    static constexpr std::chrono::milliseconds quiet_period{1500};

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    // 1 バイトでも WAL があれば閾値を超える設定。測定の間隔は短くする
    static auto_compaction_settings eager_settings() {
        auto_compaction_settings settings{};
        settings.threshold_bytes = 1;
        settings.measurement_interval = std::chrono::milliseconds(50);
        return settings;
    }

    // レプリカの datastore を作る。起動処理はまだ行わない
    void create_replica(const auto_compaction_settings& settings) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_auto_compaction_settings(settings);
        datastore_->get_impl()->set_replica_role();
    }

    void start_replica(const auto_compaction_settings& settings) {
        create_replica(settings);
        datastore_->start_as_replica();
    }

    void stop() {
        datastore_->shutdown();
        datastore_ = nullptr;
    }

    // master から 1 セッション分のログを受け取ったことにする
    static void receive_session(log_channel& channel, epoch_id_type epoch, const std::string& key,
                                const std::string& value) {
        channel.get_impl()->begin_session_at(epoch);
        channel.add_entry(1, key, value, {epoch, 0});
        channel.get_impl()->end_session_at(epoch);
    }

    // master から GROUP_COMMIT を受け取ったことにする
    void receive_group_commit(epoch_id_type epoch) {
        datastore_->persist_and_propagate_epoch_id(epoch);
        datastore_->get_impl()->set_received_epoch(epoch);
    }

    // コミット済みのコンパクションの世代 (datastore が保持している、コンパクション済み WAL ファイルの名前から求める)
    [[nodiscard]] std::uint64_t generation() const {
        const std::optional<std::string> name = datastore_->get_impl()->get_current_compacted_file_name();
        if (! name.has_value()) {
            return 0;
        }
        for (std::uint64_t candidate = 1; candidate < 100; ++candidate) {
            if (name.value() == compaction_catalog::get_compacted_filename_for_generation(candidate)) {
                return candidate;
            }
        }
        ADD_FAILURE() << "unexpected name of the compacted WAL file: " << name.value();
        return 0;
    }

    [[nodiscard]] bool wait_for_generation(std::uint64_t expected, std::chrono::milliseconds timeout) const {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (generation() >= expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return generation() >= expected;
    }

    // ログディレクトリにある、ローテーション済みの WAL ファイルの名前
    [[nodiscard]] static std::set<std::string> rotated_wal_files() {
        std::set<std::string> result{};
        for (const boost::filesystem::directory_entry& entry :
             boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("pwal_", 0) == 0 && ! limestone::internal::is_unrotated_pwal_name(name)
                && ! compaction_catalog::is_compaction_output_filename(name)
                && ! limestone::internal::locator_format::is_locator_file_name(name)) {
                result.insert(name);
            }
        }
        return result;
    }

    // master として起動し直して、snapshot が返すエントリ (キーと値) をすべて読む
    [[nodiscard]] std::vector<std::tuple<std::string, std::string>> restart_as_master_and_read() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->ready();
        std::vector<std::tuple<std::string, std::string>> result{};
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<limestone::api::cursor> cursor = snapshot->get_cursor();
        while (cursor->next()) {
            std::string key{};
            std::string value{};
            cursor->key(key);
            cursor->value(value);
            result.emplace_back(key, value);
        }
        return result;
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
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

}  // namespace limestone::testing
