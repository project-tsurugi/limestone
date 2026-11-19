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
#pragma once

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
class replica_compaction_base_test : public ::testing::Test {
public:
    explicit replica_compaction_base_test(const char* location) : location(location) {}

    const char* location;

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

    // 閾値を 1 バイトにした設定。エントリを 1 つ書けば、WAL の量が閾値を超える。測定の間隔は短くする
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
    [[nodiscard]] std::set<std::string> rotated_wal_files() const {
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

}  // namespace limestone::testing
