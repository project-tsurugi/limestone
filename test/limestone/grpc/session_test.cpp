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
#include "limestone/grpc/backend/session.h"
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>

namespace limestone::testing {

class session_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(session_test, construct_and_getters) {
    std::string id = "test_id";
    int64_t now = static_cast<int64_t>(std::time(nullptr));
    int64_t expire = now + 10;
    grpc::backend::session s(id, 1, 2,expire);
    ASSERT_EQ(s.session_id(), id);
    ASSERT_EQ(s.begin_epoch(), 1);
    ASSERT_EQ(s.end_epoch(), 2);
    ASSERT_EQ(s.expire_at(), expire);
}

TEST_F(session_test, refresh_expire_at) {
    std::string id = "test_id2";
    int64_t now = static_cast<int64_t>(std::time(nullptr));
    int64_t expire = now + 1;
    grpc::backend::session s(id, 1, 2, expire);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    s.refresh(5);
    int64_t refreshed = s.expire_at();
    ASSERT_GE(refreshed, now + 5);
}

TEST_F(session_test, call_on_remove) {
    std::atomic<bool> called{false};
    auto on_remove = [&called]() { called = true; };
    grpc::backend::session s("id3", 1, 2, std::time(nullptr) + 1, on_remove);
    s.call_on_remove();
    ASSERT_TRUE(called.load());
}


TEST_F(session_test, add_and_find_backup_object) {
    grpc::backend::session s("sid", 1, 2, 100, nullptr);
    backup_object obj1("id1", backup_object_type::log, "foo/bar");
    backup_object obj2("id2", backup_object_type::snapshot, "snap/path");
    s.add_backup_object(obj1);
    s.add_backup_object(obj2);
    auto found1 = s.find_backup_object("id1");
    ASSERT_TRUE(found1.has_value());
    ASSERT_EQ(found1->get().object_id(), "id1");
    ASSERT_EQ(found1->get().type(), backup_object_type::log);
    ASSERT_EQ(found1->get().path(), boost::filesystem::path("foo/bar"));
    auto found2 = s.find_backup_object("id2");
    ASSERT_TRUE(found2.has_value());
    ASSERT_EQ(found2->get().object_id(), "id2");
    ASSERT_EQ(found2->get().type(), backup_object_type::snapshot);
    ASSERT_EQ(found2->get().path(), boost::filesystem::path("snap/path"));
    auto not_found = s.find_backup_object("not_exist");
    ASSERT_FALSE(not_found.has_value());
}

TEST_F(session_test, add_duplicate_backup_object_throws) {
    grpc::backend::session s("sid", 1, 2, 100, nullptr);
    backup_object obj1("id1", backup_object_type::log, "foo/bar");
    s.add_backup_object(obj1);
    backup_object obj2("id1", backup_object_type::snapshot, "snap/path");
    ASSERT_THROW(s.add_backup_object(obj2), std::runtime_error);
}

TEST_F(session_test, backup_object_iteration) {
    grpc::backend::session s("sid", 1, 2, 100, nullptr);
    std::vector<std::string> ids = {"id1", "id2", "id3"};
    for (const auto& id : ids) {
        s.add_backup_object(backup_object(id, backup_object_type::log, id+"/path"));
    }
    std::vector<std::string> found_ids;
    for (auto it = s.begin(); it != s.end(); ++it) {
        found_ids.push_back(it->first);
    }
    std::sort(found_ids.begin(), found_ids.end());
    ASSERT_EQ(found_ids, ids);
}

} // namespace limestone::testing
