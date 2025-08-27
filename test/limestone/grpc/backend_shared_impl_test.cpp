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

#include "limestone/grpc/backend/backend_shared_impl.h"
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>
#include "limestone/grpc/service/message_versions.h"

namespace limestone::testing {

using limestone::grpc::proto::BackupObject;
using limestone::grpc::proto::BackupObjectType;

using namespace limestone::grpc::backend;
using namespace limestone::internal;

class backend_shared_impl_test : public ::testing::Test {
protected:
    boost::filesystem::path temp_dir;

    void SetUp() override {
        temp_dir = boost::filesystem::temp_directory_path() / boost::filesystem::unique_path();
        boost::filesystem::create_directories(temp_dir);
    }
    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }
};


TEST_F(backend_shared_impl_test, list_wal_history_returns_empty_when_dir_is_empty) {
    backend_shared_impl backend(temp_dir);
    auto result = backend.list_wal_history();
    EXPECT_TRUE(result.empty());
}

TEST_F(backend_shared_impl_test, list_wal_history_matches_wal_history_class) {
    wal_history wh(temp_dir);
    wh.append(123);
    wh.append(456);
    auto expected = wh.list();

    backend_shared_impl backend(temp_dir);
    auto actual = backend.list_wal_history();

    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].epoch, actual[i].epoch());
        EXPECT_EQ(expected[i].identity, actual[i].identity());
        EXPECT_EQ(expected[i].timestamp, actual[i].timestamp());
    }
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_metadata_files) {
    std::vector<std::string> files = {
        "compaction_catalog",
        "limestone-manifest.json",
        "wal_history",
        "epoch.1234567890.1"
    };
    for (const auto& fname : files) {
        auto obj = backend_shared_impl::make_backup_object_from_path(fname);
        ASSERT_TRUE(obj.has_value());
        EXPECT_EQ(obj->object_id(), fname);
        EXPECT_EQ(obj->path(), fname);
        EXPECT_EQ(obj->type(), BackupObjectType::METADATA);
    }
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_snapshot) {
    std::string fname = "pwal_0000.compacted";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->object_id(), fname);
    EXPECT_EQ(obj->path(), fname);
    EXPECT_EQ(obj->type(), BackupObjectType::SNAPSHOT);
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_log) {
    std::string fname = "pwal_0001.1234567890.0";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->object_id(), fname);
    EXPECT_EQ(obj->path(), fname);
    EXPECT_EQ(obj->type(), BackupObjectType::LOG);
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_not_matched) {
    std::string fname = "random_file.txt";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    EXPECT_FALSE(obj.has_value());
}

TEST_F(backend_shared_impl_test, keep_alive_success_and_not_found) {
    backend_shared_impl backend(temp_dir);
    // Create a session
    auto session_opt = backend.create_and_register_session(60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Normal case: version matches and session is valid
    limestone::grpc::proto::KeepAliveRequest req;
    req.set_version(limestone::grpc::service::keep_alive_message_version);
    req.set_session_id(session_id);
    limestone::grpc::proto::KeepAliveResponse resp;
    auto status = backend.keep_alive(&req, &resp);
    EXPECT_TRUE(status.ok());

    // Version mismatch
    req.set_version(9999);
    status = backend.keep_alive(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_STREQ(status.error_message().c_str(), "unsupported keep_alive request version");

    // Unregistered session ID
    req.set_version(limestone::grpc::service::keep_alive_message_version);
    req.set_session_id("not_found_id");
    status = backend.keep_alive(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_STREQ(status.error_message().c_str(), "session not found or expired");
}

TEST_F(backend_shared_impl_test, end_backup_success_and_not_found) {
    backend_shared_impl backend(temp_dir);
    // Create a session
    auto session_opt = backend.create_and_register_session(60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Normal case: version matches and session is valid
    limestone::grpc::proto::EndBackupRequest req;
    req.set_version(limestone::grpc::service::end_backup_message_version);
    req.set_session_id(session_id);
    limestone::grpc::proto::EndBackupResponse resp;
    auto status = backend.end_backup(&req, &resp);
    EXPECT_TRUE(status.ok());

    // Version mismatch
    req.set_version(9999);
    status = backend.end_backup(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_STREQ(status.error_message().c_str(), "unsupported end_backup request version");

    // Unregistered session ID
    req.set_version(limestone::grpc::service::end_backup_message_version);
    req.set_session_id("not_found_id");
    status = backend.end_backup(&req, &resp);
    EXPECT_TRUE(status.ok());  // remove_session returns OK even if not found
}

} // namespace limestone::testing

