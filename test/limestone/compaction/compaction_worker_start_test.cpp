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
#include <memory>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "compaction_worker_launcher.h"
#include "datastore_impl.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::internal::compaction_worker_launcher;

/**
 * オンラインコンパクションのワーカースレッドが起動する時点のテスト。ワーカースレッドは ready() では起動せず、
 * snapshot と、そこから取得した cursor がすべて破棄された時点で起動する。
 */
class compaction_worker_start_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/compaction_worker_start_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    void start_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
    }

    // エントリを 1 つ書いて再起動する。再起動の後の snapshot は、このエントリを含む
    void write_entry_and_restart() {
        datastore_->switch_epoch(1);
        lc0_->begin_session();
        lc0_->add_entry(1, "key", "value", {1, 0});
        lc0_->end_session();
        datastore_->switch_epoch(2);
        datastore_->shutdown();
        datastore_ = nullptr;
        start_datastore();
    }

    [[nodiscard]] std::shared_ptr<compaction_worker_launcher> launcher() const {
        return datastore_->get_impl()->get_compaction_worker_launcher();
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    limestone::api::log_channel* lc0_{};
};

// ready() の直後には、ワーカースレッドは起動していない
TEST_F(compaction_worker_start_test, not_started_right_after_ready) {
    start_datastore();

    ASSERT_TRUE(launcher());
    EXPECT_FALSE(launcher()->launched());
}

// ready() の後には、ワーカースレッドが起動していなくても ctrl ディレクトリがある
TEST_F(compaction_worker_start_test, ctrl_directory_exists_after_ready) {
    start_datastore();

    EXPECT_TRUE(boost::filesystem::is_directory(boost::filesystem::path(location) / "ctrl"));
    EXPECT_FALSE(launcher()->launched());
}

// snapshot と、そこから取得したすべての cursor を破棄した時点で起動する。cursor が残っている間は起動しない
TEST_F(compaction_worker_start_test, starts_when_snapshot_and_all_cursors_are_released) {
    start_datastore();
    write_entry_and_restart();

    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
    std::vector<std::unique_ptr<api::cursor>> partitioned = snapshot->get_partitioned_cursors(2);
    ASSERT_FALSE(partitioned.empty());
    EXPECT_FALSE(launcher()->launched());

    snapshot.reset();
    EXPECT_FALSE(launcher()->launched());

    ASSERT_TRUE(cursor->next());
    cursor.reset();
    EXPECT_FALSE(launcher()->launched());

    while (partitioned.size() > 1) {
        partitioned.pop_back();
        EXPECT_FALSE(launcher()->launched());
    }
    partitioned.clear();
    EXPECT_TRUE(launcher()->launched());
}

// cursor を先にすべて破棄したときは、snapshot の破棄で起動する
TEST_F(compaction_worker_start_test, starts_when_snapshot_is_released_last) {
    start_datastore();

    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
    cursor.reset();
    EXPECT_FALSE(launcher()->launched());

    snapshot.reset();
    EXPECT_TRUE(launcher()->launched());
}

// shared_snapshot() の snapshot でも、同じ時点で起動する
TEST_F(compaction_worker_start_test, starts_with_shared_snapshot) {
    start_datastore();

    std::shared_ptr<api::snapshot> snapshot = datastore_->shared_snapshot();
    std::shared_ptr<api::snapshot> copy = snapshot;
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();

    snapshot.reset();
    copy.reset();
    EXPECT_FALSE(launcher()->launched());

    cursor.reset();
    EXPECT_TRUE(launcher()->launched());
}

// 空のログディレクトリで、cursor を取得せずに snapshot を破棄しても起動する
TEST_F(compaction_worker_start_test, starts_with_empty_log_directory) {
    start_datastore();

    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
    EXPECT_FALSE(cursor->next());
    cursor.reset();
    snapshot.reset();

    EXPECT_TRUE(launcher()->launched());
}

// 起動の後に snapshot の取得と破棄を繰り返しても、正常に動く
TEST_F(compaction_worker_start_test, repeated_snapshots_after_start) {
    start_datastore();

    for (int i = 0; i < 3; ++i) {
        std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
        EXPECT_FALSE(cursor->next());
    }
    EXPECT_TRUE(launcher()->launched());

    datastore_->shutdown();
}

// snapshot を取得しないまま shutdown() しても、正常に終了する。ワーカースレッドは起動しないままである
TEST_F(compaction_worker_start_test, shutdown_without_snapshot) {
    start_datastore();
    std::shared_ptr<compaction_worker_launcher> kept = launcher();

    datastore_->shutdown();

    EXPECT_FALSE(kept->launched());
}

// shutdown() の後に snapshot と cursor を破棄しても、ワーカースレッドは起動しない
TEST_F(compaction_worker_start_test, release_after_shutdown_does_not_start) {
    start_datastore();
    std::shared_ptr<compaction_worker_launcher> kept = launcher();
    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();

    datastore_->shutdown();
    cursor.reset();
    snapshot.reset();

    EXPECT_FALSE(kept->launched());
}

// datastore を snapshot より先に破棄しても、異常が起きない
TEST_F(compaction_worker_start_test, datastore_destroyed_before_snapshot) {
    start_datastore();
    std::shared_ptr<compaction_worker_launcher> kept = launcher();
    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();

    datastore_ = nullptr;
    cursor.reset();
    snapshot.reset();

    EXPECT_FALSE(kept->launched());
}

// snapshot の破棄と shutdown() を並行させても、待たれないワーカースレッドが残らない。
// 残った場合は、破棄済みの datastore に触れるか、テストプロセスの終了を妨げる
TEST_F(compaction_worker_start_test, release_concurrent_with_shutdown) {
    for (int i = 0; i < 20; ++i) {
        start_datastore();
        std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();

        std::thread releaser([&snapshot]() { snapshot.reset(); });
        datastore_->shutdown();
        releaser.join();

        datastore_ = nullptr;
    }
}

}  // namespace limestone::testing
