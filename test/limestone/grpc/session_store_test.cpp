/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include "limestone/grpc/backend/session_store.h"
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>

namespace limestone::testing {

class session_store_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(session_store_test, create_and_register_and_get_and_remove) {
    grpc::backend::session_store store;
    std::atomic<bool> removed{false};
    auto on_remove = [&removed]() { removed = true; };
    int64_t before_create = static_cast<int64_t>(std::time(nullptr));
    auto s_opt = store.create_and_register(2, on_remove);
    ASSERT_TRUE(s_opt.has_value());
    ASSERT_GE(s_opt->expire_at(), before_create + 2);
    std::string session_id = s_opt->session_id();

    // get_and_refresh should succeed
    int64_t before = static_cast<int64_t>(std::time(nullptr));
    auto s2 = store.get_and_refresh(session_id, 2);
    ASSERT_TRUE(s2.has_value());
    ASSERT_EQ(s2->session_id(), s_opt->session_id());
    ASSERT_GE(s2->expire_at(), before + 2);
    // remove_session should succeed
    bool removed_result = store.remove_session(session_id);
    ASSERT_TRUE(removed_result);
    ASSERT_TRUE(removed.load());
    // remove_session should fail (already removed)
    bool removed_result2 = store.remove_session(session_id);
    ASSERT_FALSE(removed_result2);
    // get_and_refresh should fail after removal
    auto s3 = store.get_and_refresh(session_id, 2);
    ASSERT_FALSE(s3.has_value());
}

TEST_F(session_store_test, session_expiry) {
    grpc::backend::session_store store;
    std::atomic<bool> removed{false};
    std::mutex mtx;
    std::condition_variable cv;
    auto on_remove = [&removed, &cv, &mtx]() {
        {
            std::lock_guard<std::mutex> lock(mtx);
            removed = true;
        }
        cv.notify_one();
    };
    auto s_opt = store.create_and_register(0, on_remove);
    ASSERT_TRUE(s_opt.has_value());
    std::string session_id = s_opt->session_id();
    // wait for expiry (on_remove called)
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait_for(lock, std::chrono::seconds(3), [&removed]{ return removed.load(); });
    }
    auto s2 = store.get_and_refresh(session_id, 1);
    ASSERT_FALSE(s2.has_value());
    ASSERT_TRUE(removed.load());
}

TEST_F(session_store_test, get_and_refresh_expired_session) {
    grpc::backend::session_store store;
    std::atomic<bool> removed{false};
    auto on_remove = [&removed]() { removed = true; };
    auto s_opt = store.create_and_register(0, on_remove);
    ASSERT_TRUE(s_opt.has_value());
    std::string session_id = s_opt->session_id();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto s2 = store.get_and_refresh(session_id, 1);
    ASSERT_FALSE(s2.has_value());
    ASSERT_TRUE(removed.load());
}

TEST_F(session_store_test, expiry_thread_waits_for_next_expire) {
    grpc::backend::session_store store;
    std::atomic<int> removed_count{0};
    std::mutex mtx;
    std::condition_variable cv;
    auto on_remove = [&removed_count, &cv, &mtx]() {
        {
            std::lock_guard<std::mutex> lock(mtx);
            ++removed_count;
        }
        cv.notify_all();
    };
    // Register two sessions with different expire_at values
    auto s1 = store.create_and_register(1, on_remove);
    auto s2 = store.create_and_register(3, on_remove);
    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    // Check the state immediately after the first expire
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait_for(lock, std::chrono::seconds(2), [&removed_count]{ return removed_count.load() >= 1; });
    }
    // Check the state immediately after the second expire
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait_for(lock, std::chrono::seconds(3), [&removed_count]{ return removed_count.load() >= 2; });
    }
    ASSERT_EQ(removed_count.load(), 2);
}

} // namespace limestone::testing

