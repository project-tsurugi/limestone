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
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "auto_compaction_settings.h"
#include "compaction_catalog.h"
#include "datastore_impl.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::internal::auto_compaction_settings;
using limestone::internal::compaction_catalog;

/**
 * 常時コンパクションのテスト。オンラインコンパクションのワーカースレッドが、WAL の量が閾値を超えたときに
 * ローテーションとコンパクションを実行することを、カタログの世代の変化で確かめる。
 */
class auto_compaction_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/auto_compaction_test";

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

    // datastore を起動する。ワーカースレッドはまだ起動しない
    void start_datastore(const auto_compaction_settings& settings) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_auto_compaction_settings(settings);
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
    }

    // snapshot を取得して破棄し、ワーカースレッドを起動する
    void start_worker() {
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
    }

    // 1 バイトでも WAL があれば閾値を超える設定。測定の間隔は短くする
    static auto_compaction_settings eager_settings() {
        auto_compaction_settings settings{};
        settings.threshold_bytes = 1;
        settings.measurement_interval = std::chrono::milliseconds(50);
        return settings;
    }

    // epoch を 1 つ進める。ローテーションは、境界 epoch までの永続化の完了を待つ。実際の運用では上位が epoch を
    // 進め続けるので、テストでも待つ間は進め続ける
    void advance_epoch() {
        ++epoch_;
        datastore_->switch_epoch(epoch_);
    }

    // 現在の epoch でエントリを 1 つ書き、epoch を進める
    void write_entry(const std::string& key, const std::string& value) {
        lc0_->begin_session();
        lc0_->add_entry(1, key, value, {epoch_, 0});
        lc0_->end_session();
        advance_epoch();
    }

    // epoch を進めながら、指定の時間だけ待つ
    void advance_epochs_for(std::chrono::milliseconds duration) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline) {
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    [[nodiscard]] static std::uint64_t catalog_generation() {
        return compaction_catalog::from_catalog_file(boost::filesystem::path(location)).get_generation();
    }

    // epoch を進めながら、カタログの世代が expected 以上になるまで待つ。timeout までにならなければ false を返す
    [[nodiscard]] bool wait_for_generation(std::uint64_t expected, std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (catalog_generation() >= expected) {
                return true;
            }
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return catalog_generation() >= expected;
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    limestone::api::log_channel* lc0_{};
    epoch_id_type epoch_{0};
};

// WAL の量が閾値を超えると、常時コンパクションが実行される
TEST_F(auto_compaction_test, runs_when_threshold_is_exceeded) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    ASSERT_EQ(catalog_generation(), 0U);

    start_worker();

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// 閾値が 0 (既定) のときは、常時コンパクションは実行されない
TEST_F(auto_compaction_test, does_not_run_with_zero_threshold) {
    auto_compaction_settings settings{};
    settings.measurement_interval = std::chrono::milliseconds(50);
    ASSERT_EQ(settings.threshold_bytes, 0U);
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
}

// WAL の量が閾値以下のときは、常時コンパクションは実行されない
TEST_F(auto_compaction_test, does_not_run_below_threshold) {
    auto_compaction_settings settings = eager_settings();
    settings.threshold_bytes = std::uint64_t{1024} * 1024;
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
}

// WAL ファイルが 1 つもない状態では、何も実行されず、誤りにもならない
TEST_F(auto_compaction_test, does_nothing_without_wal_files) {
    start_datastore(eager_settings());
    advance_epoch();

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
    datastore_->shutdown();
}

// ローテーションの境界 epoch が 0 の間は実行されない。epoch が進むと実行される
TEST_F(auto_compaction_test, waits_while_boundary_epoch_is_zero) {
    start_datastore(eager_settings());
    // 閾値を超える量の、WAL ファイルの名前を持つファイルを置く。中身は読まれない
    {
        boost::filesystem::ofstream stream(boost::filesystem::path(location) / "pwal_0009.1700000000000.0",
                                           std::ios::binary);
        stream << "x";
    }
    ASSERT_EQ(datastore_->epoch_id_switched(), 0U);

    start_worker();
    std::this_thread::sleep_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    boost::filesystem::remove(boost::filesystem::path(location) / "pwal_0009.1700000000000.0");
    advance_epoch();
    write_entry("k1", "v1");

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// 測定は、測定の間隔より短い間隔では行われない
TEST_F(auto_compaction_test, measures_at_the_measurement_interval) {
    auto_compaction_settings settings = eager_settings();
    settings.measurement_interval = std::chrono::seconds(4);
    start_datastore(settings);
    advance_epoch();

    // 最初の測定は、起動の直後に行われる。この時点では WAL がない
    start_worker();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    write_entry("k1", "v1");

    // 次の測定までは、閾値を超えていても実行されない
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// バックアップ中は、常時コンパクションは実行されない。バックアップが終わると実行される
TEST_F(auto_compaction_test, does_not_run_during_backup) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    datastore_->get_impl()->increment_backup_counter();

    start_worker();
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    datastore_->get_impl()->decrement_backup_counter();
    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// ctrl ファイルの指示と閾値の超過が重なっても、順に処理される
TEST_F(auto_compaction_test, ctrl_file_request_and_threshold_are_handled_in_order) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    const boost::filesystem::path start_file = boost::filesystem::path(location) / "ctrl" / "start_compaction";
    {
        boost::filesystem::ofstream stream(start_file);
    }

    start_worker();

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    EXPECT_FALSE(boost::filesystem::exists(start_file));

    // 続けて書いた分も、次の測定でコンパクションされる
    write_entry("k2", "v2");
    EXPECT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// 常時コンパクションの実行中に shutdown() しても、正常に終了する
TEST_F(auto_compaction_test, shutdown_while_worker_is_running) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    datastore_->shutdown();
}

}  // namespace limestone::testing
