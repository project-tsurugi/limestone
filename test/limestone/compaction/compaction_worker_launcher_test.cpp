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
#include <memory>
#include <thread>
#include <vector>

#include "test_root.h"

#include "compaction_worker_launcher.h"

namespace limestone::testing {

using limestone::internal::compaction_worker_launcher;
using limestone::internal::snapshot_use_marker;

// The start function is called only once even if launch() is repeated
TEST(compaction_worker_launcher_test, launch_starts_only_once) {
    std::atomic<int> started{0};
    compaction_worker_launcher launcher([&started]() { started.fetch_add(1); });
    EXPECT_FALSE(launcher.launched());

    launcher.launch();
    launcher.launch();

    EXPECT_EQ(started.load(), 1);
    EXPECT_TRUE(launcher.launched());
}

// launch() after close() does nothing
TEST(compaction_worker_launcher_test, launch_does_nothing_after_close) {
    std::atomic<int> started{0};
    compaction_worker_launcher launcher([&started]() { started.fetch_add(1); });

    launcher.close();
    launcher.launch();

    EXPECT_EQ(started.load(), 0);
    EXPECT_FALSE(launcher.launched());
}

// close() after launch() does not change the started state
TEST(compaction_worker_launcher_test, close_keeps_launched) {
    std::atomic<int> started{0};
    compaction_worker_launcher launcher([&started]() { started.fetch_add(1); });

    launcher.launch();
    launcher.close();

    EXPECT_EQ(started.load(), 1);
    EXPECT_TRUE(launcher.launched());
}

// The start function is called only once even if launch() is called from several threads at the same time
TEST(compaction_worker_launcher_test, concurrent_launch_starts_only_once) {
    std::atomic<int> started{0};
    compaction_worker_launcher launcher([&started]() { started.fetch_add(1); });

    std::vector<std::thread> threads{};
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&launcher]() { launcher.launch(); });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(started.load(), 1);
}

// The marker requests the start when its last owner releases it
TEST(compaction_worker_launcher_test, marker_launches_when_last_owner_releases) {
    std::atomic<int> started{0};
    std::shared_ptr<compaction_worker_launcher> launcher =
        std::make_shared<compaction_worker_launcher>([&started]() { started.fetch_add(1); });

    std::shared_ptr<snapshot_use_marker> first = std::make_shared<snapshot_use_marker>(launcher);
    std::shared_ptr<snapshot_use_marker> second = first;

    first.reset();
    EXPECT_EQ(started.load(), 0);

    second.reset();
    EXPECT_EQ(started.load(), 1);
}

// A marker destroyed after close() does not call the start function
TEST(compaction_worker_launcher_test, marker_does_nothing_after_close) {
    std::atomic<int> started{0};
    std::shared_ptr<compaction_worker_launcher> launcher =
        std::make_shared<compaction_worker_launcher>([&started]() { started.fetch_add(1); });

    std::shared_ptr<snapshot_use_marker> marker = std::make_shared<snapshot_use_marker>(launcher);
    launcher->close();
    marker.reset();

    EXPECT_EQ(started.load(), 0);
}

}  // namespace limestone::testing
