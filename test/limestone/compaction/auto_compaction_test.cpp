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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/backup_detail.h>
#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "compaction_settings.h"
#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "file_operations.h"
#include "internal.h"
#include "locator/locator_format.h"
#include "manifest.h"

namespace limestone::testing {

namespace {

// 指定した名前のファイルの削除だけを失敗させる file_operations
class remove_failing_file_operations : public limestone::internal::real_file_operations {
public:
    void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
        {
            std::scoped_lock<std::mutex> lock(mutex_);
            if (path.filename().string() == failing_name_) {
                ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
                return;
            }
        }
        real_file_operations::remove(path, ec);
    }

    void fail_for(const std::string& name) {
        {
            std::scoped_lock<std::mutex> lock(mutex_);
            failing_name_ = name;
        }
    }

private:
    std::mutex mutex_{};
    std::string failing_name_{};
};

}  // namespace

using limestone::api::epoch_id_type;
using limestone::internal::compaction_settings;
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
        limestone::api::datastore_impl::set_file_operations_for_test(nullptr);
        boost::filesystem::remove_all(location);
    }

    // datastore を起動する。ワーカースレッドはまだ起動しない
    void start_datastore(const compaction_settings& settings) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_compaction_settings(settings);
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
    }

    // snapshot を取得して破棄し、ワーカースレッドを起動する
    void start_worker() {
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
    }

    // 閾値を 1 バイトにした設定。エントリを 1 つ書けば、WAL の量が閾値を超える。測定の間隔は短くする
    static compaction_settings eager_settings() {
        compaction_settings settings{};
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

    // コミット済みのコンパクションの世代。datastore が保持している、コンパクション済み WAL ファイルの名前から
    // 求める。カタログファイルは読まない (ワーカースレッドが書き換えている最中のファイルを読むと、壊れていると
    // 判断してバックアップから復元してしまう)
    [[nodiscard]] std::uint64_t catalog_generation() const {
        const std::optional<std::string> name = datastore_->get_impl()->get_current_compacted_file_name();
        if (! name.has_value()) {
            return 0;
        }
        for (std::uint64_t generation = 1; generation < 100; ++generation) {
            if (name.value() == compaction_catalog::get_compacted_filename_for_generation(generation)) {
                return generation;
            }
        }
        ADD_FAILURE() << "unexpected name of the compacted WAL file: " << name.value();
        return 0;
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

    // epoch を進めながら、ローテーション済みの WAL ファイルが expected と一致するまで待つ
    [[nodiscard]] bool wait_for_rotated_wal_files(const std::set<std::string>& expected,
                                                  std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (rotated_wal_files() == expected) {
                return true;
            }
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return rotated_wal_files() == expected;
    }

    // ctrl ファイルの指示と同じコンパクションを、ワーカースレッドを通さずに実行する
    void run_requested_compaction() {
        std::future<void> future = std::async(std::launch::async, [this]() { datastore_->compact_with_online(); });
        while (future.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
            advance_epoch();
        }
        future.get();
    }

    // files_ (バックアップが列挙するファイル) に、ローテーション済みの WAL ファイルがいくつあるか
    [[nodiscard]] std::size_t rotated_wal_files_in_backup_targets() const {
        std::size_t count = 0;
        for (const boost::filesystem::path& path : datastore_->files()) {
            const std::string name = path.filename().string();
            if (name.rfind("pwal_", 0) == 0 && ! limestone::internal::is_unrotated_pwal_name(name)
                && ! compaction_catalog::is_compaction_output_filename(name)
                && ! limestone::internal::locator_format::is_locator_file_name(name)) {
                ++count;
            }
        }
        return count;
    }

    // snapshot が返すエントリ (キーと値) をすべて読む
    [[nodiscard]] std::vector<std::tuple<std::string, std::string>> read_all_entries() {
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
    compaction_settings settings{};
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
    compaction_settings settings = eager_settings();
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
        stream << "the content is not read; it only has to be larger than the threshold";
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
    compaction_settings settings = eager_settings();
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

// 形式バージョンの移行に伴うローテーションでリネームした WAL ファイルは、その起動の中で、バックアップの対象と
// 常時コンパクションの入力になる。入力にならないと、WAL の量が閾値を下回らず、測定のたびにコンパクションが
// 繰り返される
TEST_F(auto_compaction_test, wal_files_rotated_for_format_version_migration_are_compacted) {
    start_datastore(compaction_settings{});
    advance_epoch();
    write_entry("k1", "v1");
    datastore_->shutdown();
    datastore_ = nullptr;

    limestone::api::configuration conf{};
    conf.set_data_location(location);
    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    datastore_->get_impl()->set_compaction_settings(eager_settings());
    datastore_->get_impl()->set_migration_info(limestone::internal::manifest::migration_info(5, 6));
    lc0_ = &datastore_->create_channel();
    datastore_->ready();
    EXPECT_EQ(rotated_wal_files().size(), 1U);
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 1U);

    start_worker();
    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    // リネームしたファイルは、入力になった後、削除される。残っていれば、次の測定でもう一度実行される
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 1U);
    EXPECT_TRUE(rotated_wal_files().empty());
    datastore_->shutdown();
}

// 終了する。コンパクションは行われない
TEST_F(auto_compaction_test, shutdown_while_rotation_is_waiting) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    // セッションを終了しないので、ローテーションは永続化の完了を待ち続ける
    lc0_->begin_session();
    lc0_->add_entry(1, "k2", "v2", {epoch_, 0});
    std::atomic<bool> rotation_started{false};
    datastore_->on_rotate_log_files_callback = [&rotation_started]() { rotation_started.store(true); };

    start_worker();
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (! rotation_started.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(rotation_started.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    datastore_->shutdown();

    EXPECT_EQ(catalog_generation(), 0U);
    EXPECT_TRUE(rotated_wal_files().empty());
}

// 常時コンパクションは、既定では detached の WAL ファイルを削除する。削除したファイルは、バックアップの対象から外れる
TEST_F(auto_compaction_test, removes_detached_wal_files_by_default) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 0U);
}

// 削除の設定を無効にすると、detached の WAL ファイルは残る
TEST_F(auto_compaction_test, keeps_detached_wal_files_when_disabled) {
    compaction_settings settings = eager_settings();
    settings.delete_detached = false;
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    datastore_->shutdown();
    EXPECT_EQ(rotated_wal_files().size(), 1U);
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 1U);
}

// ctrl ファイルの指示によるコンパクションは、detached の WAL ファイルを削除しない。残ったファイルは、次の
// 常時コンパクションが削除する
TEST_F(auto_compaction_test, removes_wal_files_detached_by_earlier_compactions) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    run_requested_compaction();
    ASSERT_EQ(catalog_generation(), 1U);
    ASSERT_EQ(rotated_wal_files().size(), 1U);

    write_entry("k2", "v2");
    start_worker();

    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// コンパクションの実行中に始めたバックアップは、コンパクションの完了を待つ。バックアップの対象には、
// コンパクションが削除したファイルが入らない
TEST_F(auto_compaction_test, backup_waits_for_running_compaction) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    // コンパクションを、カタログの更新の後、削除の前で止める
    std::promise<void> reached{};
    std::promise<void> release{};
    std::shared_future<void> released = release.get_future().share();
    std::atomic<bool> fired{false};
    datastore_->get_impl()->set_on_compaction_after_commit_for_test([&reached, released, &fired]() {
        if (! fired.exchange(true)) {
            reached.set_value();
            released.wait();
        }
    });

    start_worker();
    std::future<void> reached_future = reached.get_future();
    while (reached_future.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
        advance_epoch();
    }
    const std::set<std::string> detached = rotated_wal_files();
    ASSERT_EQ(detached.size(), 1U);

    std::future<std::unique_ptr<limestone::api::backup_detail>> backup = std::async(
        std::launch::async, [this]() { return datastore_->begin_backup(limestone::api::backup_type::standard); });
    advance_epochs_for(std::chrono::milliseconds(300));
    EXPECT_NE(backup.wait_for(std::chrono::milliseconds(0)), std::future_status::ready);

    release.set_value();
    while (backup.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
        advance_epoch();
    }
    std::unique_ptr<limestone::api::backup_detail> detail = backup.get();
    for (const limestone::api::backup_detail::entry& entry : detail->entries()) {
        EXPECT_EQ(detached.count(entry.source_path().filename().string()), 0U);
        EXPECT_TRUE(boost::filesystem::exists(entry.source_path()));
    }
    for (const std::string& name : detached) {
        EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / name));
    }
    detail->notify_end_backup();
    datastore_->shutdown();
}

// 削除に失敗したファイルがあっても、残りは削除される。失敗したファイルは、次の常時コンパクションが削除する
TEST_F(auto_compaction_test, continues_after_a_removal_failure) {
    remove_failing_file_operations ops{};
    limestone::api::datastore_impl::set_file_operations_for_test(&ops);
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    run_requested_compaction();
    const std::set<std::string> left = rotated_wal_files();
    ASSERT_EQ(left.size(), 1U);
    ops.fail_for(*left.begin());

    write_entry("k2", "v2");
    start_worker();

    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files(left, std::chrono::seconds(10)));

    ops.fail_for("");
    write_entry("k3", "v3");

    ASSERT_TRUE(wait_for_generation(3, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// detached の WAL ファイルを削除した後の永続化データで、次のコンパクション、再起動、tglogutil の inspect が
// 正常に動く
TEST_F(auto_compaction_test, data_is_usable_after_removal) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    start_worker();
    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    ASSERT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));

    // 次のコンパクション。カタログの detached の一覧には、削除したファイルの名前が残っている
    write_entry("k2", "v2");
    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    ASSERT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
    datastore_ = nullptr;

    // tglogutil の inspect
    {
        const std::string command = std::string("../src/tglogutil inspect ") + location + " 2>&1";
        FILE* pipe = popen(command.c_str(), "r");
        ASSERT_NE(pipe, nullptr);
        std::string output{};
        std::array<char, 4096> buffer{};
        std::size_t read_size = 0;
        while ((read_size = fread(buffer.data(), 1, buffer.size(), pipe)) > 0) {
            output.append(buffer.data(), read_size);
        }
        EXPECT_EQ(pclose(pipe), 0) << output;
    }

    // 再起動
    compaction_settings settings{};
    start_datastore(settings);
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}, {"k2", "v2"}};
    EXPECT_EQ(read_all_entries(), expected);
}

}  // namespace limestone::testing
