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
 * Tests of when the online compaction worker thread starts. The worker thread does not start in ready(); it
 * starts when a snapshot and all the cursors obtained from it are destroyed.
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

    // Writes one entry and restarts. The snapshot after the restart contains this entry
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

// The worker thread has not started right after ready()
TEST_F(compaction_worker_start_test, not_started_right_after_ready) {
    start_datastore();

    ASSERT_TRUE(launcher());
    EXPECT_FALSE(launcher()->launched());
}

// The ctrl directory exists after ready() even though the worker thread has not started
TEST_F(compaction_worker_start_test, ctrl_directory_exists_after_ready) {
    start_datastore();

    EXPECT_TRUE(boost::filesystem::is_directory(boost::filesystem::path(location) / "ctrl"));
    EXPECT_FALSE(launcher()->launched());
}

// It starts when the snapshot and all the cursors obtained from it are destroyed. It does not start while a
// cursor remains
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

// When all the cursors are destroyed first, it starts at the destruction of the snapshot
TEST_F(compaction_worker_start_test, starts_when_snapshot_is_released_last) {
    start_datastore();

    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
    cursor.reset();
    EXPECT_FALSE(launcher()->launched());

    snapshot.reset();
    EXPECT_TRUE(launcher()->launched());
}

// It starts at the same point with a snapshot of shared_snapshot()
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

// It also starts when, with an empty log directory, the snapshot is destroyed without obtaining a cursor
TEST_F(compaction_worker_start_test, starts_with_empty_log_directory) {
    start_datastore();

    std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
    std::unique_ptr<api::cursor> cursor = snapshot->get_cursor();
    EXPECT_FALSE(cursor->next());
    cursor.reset();
    snapshot.reset();

    EXPECT_TRUE(launcher()->launched());
}

// Creating and destroying snapshots repeatedly after the start works normally
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

// shutdown() without creating a snapshot completes normally. The worker thread stays unstarted
TEST_F(compaction_worker_start_test, shutdown_without_snapshot) {
    start_datastore();
    std::shared_ptr<compaction_worker_launcher> kept = launcher();

    datastore_->shutdown();

    EXPECT_FALSE(kept->launched());
}

// Destroying the snapshot and the cursors after shutdown() does not start the worker thread
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

// Destroying the datastore before the snapshot causes no anomaly
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

// Destroying a snapshot concurrently with shutdown() leaves no worker thread that nobody waits for.
// If one were left, it would touch the destroyed datastore or keep the test process from exiting
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
