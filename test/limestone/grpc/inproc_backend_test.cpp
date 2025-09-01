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

#include "limestone/grpc/backend/inproc_backend.h"

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <regex>
#include <vector>

#include "backend_test_fixture.h"
#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "limestone/api/configuration.h"
#include "limestone/api/datastore.h"
#include "limestone/blob/blob_test_helpers.h"
#include "limestone/grpc/service/grpc_constants.h"
#include "limestone/grpc/service/message_versions.h"
#include "test_root.h"
#include "wal_sync/wal_history.h"

namespace limestone::testing {

using limestone::grpc::backend::inproc_backend;
using limestone::grpc::proto::BackupObjectType;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::list_wal_history_message_version;
using limestone::internal::blob_file_resolver;
using limestone::internal::wal_history;
using limestone::grpc::service::session_timeout_seconds;

namespace {
// Structure representing backup file conditions for backup test
struct backup_condition {
    std::string path_pre_begin_backup;
    std::string path_post_begin_backup;
    std::string object_id;
    std::string object_path;
    BackupObjectType object_type;
};

static const std::vector<backup_condition> backup_conditions = {
    {"blob/dir_00/00000000000000c8.blob", "blob/dir_00/00000000000000c8.blob", "", "", BackupObjectType::UNSPECIFIED},
    {"compaction_catalog", "compaction_catalog", "compaction_catalog", "compaction_catalog", BackupObjectType::METADATA},
    {"compaction_catalog.back", "compaction_catalog.back", "", "", BackupObjectType::UNSPECIFIED},
    {"data/snapshot", "data/snapshot", "", "", BackupObjectType::UNSPECIFIED},
    {"epoch", "epoch", "", "", BackupObjectType::UNSPECIFIED}, 
    {"", "epoch.*.6", "epoch.*.6", "epoch.*.6", BackupObjectType::METADATA},
    {"limestone-manifest.json", "limestone-manifest.json", "limestone-manifest.json", "limestone-manifest.json", BackupObjectType::METADATA},
    {"pwal_0000.*.0", "pwal_0000.*.0", "pwal_0000.*.0", "pwal_0000.*.0", BackupObjectType::LOG},
    {"pwal_0000.compacted", "pwal_0000.compacted", "pwal_0000.compacted", "pwal_0000.compacted", BackupObjectType::SNAPSHOT},
    {"pwal_0001", "pwal_0001.*.0", "pwal_0001.*.0", "pwal_0001.*.0", BackupObjectType::LOG},
    {"wal_history", "wal_history", "wal_history", "wal_history", BackupObjectType::METADATA}
};
} // anonymous namespace





class inproc_backend_test : public backend_test_fixture {
protected:
    template <typename Selector>
    void assert_backup_file_conditions(Selector selector) {
        namespace fs = boost::filesystem;
        fs::path dir(get_location());

        std::set<std::string> actual;
        for (auto& entry : fs::recursive_directory_iterator(dir)) {
            if (fs::is_regular_file(entry.status())) {
                fs::path rel = fs::relative(entry.path(), dir);
                std::string relstr = rel.generic_string();
                actual.insert(relstr);
            }
        }


        // Check for expected files
        for (const auto& cond : backup_conditions) {
            const std::string& pattern = selector(cond);
            if (pattern.empty()) {
                // If empty, the file must NOT exist
                bool found = false;
                for (const auto& act : actual) {
                    if (act.empty()) {
                        found = true;
                        break;
                    }
                }
                ASSERT_FALSE(found) << "Unexpected file found (should not exist): <empty pattern>";
                continue;
            }
            bool found = false;
            std::string regex_pat = wildcard_to_regex(pattern);
            std::regex re(regex_pat);
            for (const auto& act : actual) {
                if (std::regex_match(act, re)) {
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found) << "Expected file pattern not found: " << pattern;
        }

        // Check for unexpected files
        for (const auto& act : actual) {
            bool matched = false;
            for (const auto& cond : backup_conditions) {
                const std::string& pattern = selector(cond);
                if (pattern.empty()) {
                    continue;
                }
                std::string regex_pat = wildcard_to_regex(pattern);
                std::regex re(regex_pat);
                if (std::regex_match(act, re)) {
                    matched = true;
                    break;
                }
            }
            ASSERT_TRUE(matched) << "Unexpected file found: " << act;
        }
    }
    
    const char* get_location() const override { return "/tmp/inproc_backend_test"; }
};

TEST_F(inproc_backend_test, get_wal_history_response_empty) {
    gen_datastore(call_ready_mode::call_ready_manual);
    inproc_backend backend(*datastore_, get_location());
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    WalHistoryResponse response;
    auto status = backend.get_wal_history_response(&request, &response);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(response.records_size(), 0);
}


TEST_F(inproc_backend_test, get_wal_history_response_with_records) {
    gen_datastore();
    wal_history wh(get_location());
    wh.append(300);
    wh.append(400);
    datastore_->switch_epoch(401);
    auto expected = wh.list();
    inproc_backend backend(*datastore_, get_location());
    WalHistoryRequest request;  
    request.set_version(list_wal_history_message_version);
    WalHistoryResponse response;
    auto status = backend.get_wal_history_response(&request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(expected.size(), response.records_size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(response.last_epoch(), 400);
}

TEST_F(inproc_backend_test, get_log_dir_returns_constructor_value) {
    gen_datastore();
    inproc_backend backend(*datastore_, get_location());
    EXPECT_EQ(backend.get_log_dir(), get_location());
}

TEST_F(inproc_backend_test, get_wal_history_response_version_boundary) {
    gen_datastore();
    inproc_backend backend(*datastore_, get_location());
    WalHistoryRequest request;
    WalHistoryResponse response;

    request.set_version(0);
    auto status = backend.get_wal_history_response(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);

    request.set_version(list_wal_history_message_version);
    status = backend.get_wal_history_response(&request, &response);
    EXPECT_TRUE(status.ok());

    request.set_version(2);
    status = backend.get_wal_history_response(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(inproc_backend_test, begin_backup_version_boundary) {
    gen_datastore();
    prepare_backup_test_files();

    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;

    // version=0 (unsupported)
    request.set_version(0);
    request.set_begin_epoch(0);
    request.set_end_epoch(0);
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);

    // version=1 (supported, but not implemented)
    request.set_version(begin_backup_message_version);
    status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
    
    // version=2 (unsupported)
    request.set_version(2);
    status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(inproc_backend_test, begin_backup_overall) {
    gen_datastore();
    prepare_backup_test_files();
    assert_backup_file_conditions([](const backup_condition& c) { return c.path_pre_begin_backup; });

    // FLAGS_v = 50; // set VLOG level to 50

    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    request.set_version(1);
    request.set_begin_epoch(0);
    request.set_end_epoch(0);
    BeginBackupResponse response;
    
    // Call begin_backup via run_with_epoch_switch to synchronize with epoch switch and log rotation if needed
    auto before = static_cast<int64_t>(std::time(nullptr));
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    auto after = static_cast<int64_t>(std::time(nullptr));

    // Check log_dir after begin_backup
    assert_backup_file_conditions([](const backup_condition& c) { return c.path_post_begin_backup; });

    // Check that session_id is a valid UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    const std::string& session_id = response.session_id();
    std::regex uuid_regex(R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");
    EXPECT_TRUE(std::regex_match(session_id, uuid_regex)) << "session_id is not a valid UUID: " << session_id;

    // expire_at should be in [before + session_timeout_seconds, after + session_timeout_seconds]
    int64_t expire_at = response.expire_at();
    EXPECT_GE(expire_at, before + session_timeout_seconds);
    EXPECT_LE(expire_at, after + session_timeout_seconds);
    EXPECT_EQ(response.start_epoch(), 0);
    EXPECT_EQ(response.finish_epoch(), 0);

    // --- 既存: response.objects() の内容検証 ---
    for (const auto& cond : backup_conditions) {
        if (cond.object_id.empty() || cond.object_path.empty() || cond.object_type == BackupObjectType::UNSPECIFIED) {
            continue; // Skip conditions that do not specify object details
        }
        bool found = false;
        std::regex id_re(wildcard_to_regex(cond.object_id));
        std::regex path_re(wildcard_to_regex(cond.object_path));
        for (const auto& obj : response.objects()) {
            if (std::regex_match(obj.object_id(), id_re) &&
                std::regex_match(obj.path(), path_re) &&
                obj.type() == cond.object_type) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "BackupObject not found: id=" << cond.object_id << ", path=" << cond.object_path << ", type=" << cond.object_type;
    }
    // If there is an object in response.objects() that does not exist in backup_conditions, report an error
    for (const auto& obj : response.objects()) {
        bool found = false;
        for (const auto& cond : backup_conditions) {
            if (cond.object_id.empty() || cond.object_path.empty() || cond.object_type == BackupObjectType::UNSPECIFIED) {
                continue;
            }
            std::regex id_re(wildcard_to_regex(cond.object_id));
            std::regex path_re(wildcard_to_regex(cond.object_path));
            if (std::regex_match(obj.object_id(), id_re) &&
                std::regex_match(obj.path(), path_re) &&
                obj.type() == cond.object_type) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Unexpected BackupObject found: id=" << obj.object_id() << ", path=" << obj.path() << ", type=" << obj.type();
    }

    const auto& session_store = backend.get_backend_shared_impl().get_session_store();
    auto session_opt = session_store.get_session(session_id);
    ASSERT_TRUE(session_opt.has_value()) << "Session not found for session_id: " << session_id;
    const auto& session = session_opt.value();

    // セッションの backup_object map から proto型に変換して比較
    std::vector<limestone::grpc::proto::BackupObject> session_objects;
    for (auto it = session.begin(); it != session.end(); ++it) {
        session_objects.push_back(it->second.to_proto());
    }
    // 数が一致すること
    EXPECT_EQ(session_objects.size(), static_cast<size_t>(response.objects_size()));
    // 各要素が一致すること（順序は問わない）
    for (const auto& obj : session_objects) {
        bool found = false;
        for (const auto& resp_obj : response.objects()) {
            if (obj.object_id() == resp_obj.object_id() &&
                obj.path() == resp_obj.path() &&
                obj.type() == resp_obj.type()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Session backup_object not found in response: id=" << obj.object_id() << ", path=" << obj.path() << ", type=" << obj.type();
    }
    // 逆も同様に
    for (const auto& resp_obj : response.objects()) {
        bool found = false;
        for (const auto& obj : session_objects) {
            if (obj.object_id() == resp_obj.object_id() &&
                obj.path() == resp_obj.path() &&
                obj.type() == resp_obj.type()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Response backup_object not found in session: id=" << resp_obj.object_id() << ", path=" << resp_obj.path() << ", type=" << resp_obj.type();
    }

    EXPECT_TRUE(status.ok());
}

// begin_epoch > end_epoch
TEST_F(inproc_backend_test, begin_backup_epoch_order_ok) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(4);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}

TEST_F(inproc_backend_test, begin_backup_epoch_order_equal_ng) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(3);
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be less than end_epoch: begin_epoch=3, end_epoch=3");
}

TEST_F(inproc_backend_test, begin_backup_epoch_order_gt_ng) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(4);
    request.set_end_epoch(3);
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be less than end_epoch: begin_epoch=4, end_epoch=3");
}

TEST_F(inproc_backend_test, begin_backup_begin_epoch_gt_snapshot_ok) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(3);
    request.set_end_epoch(4);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}

TEST_F(inproc_backend_test, begin_backup_begin_epoch_eq_snapshot_ng) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(2);
    request.set_end_epoch(4);
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be strictly greater than the epoch id of the last snapshot: begin_epoch=2, snapshot_epoch_id=2");
}

TEST_F(inproc_backend_test, begin_backup_begin_epoch_lt_snapshot_ng) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(1);
    request.set_end_epoch(4);
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be strictly greater than the epoch id of the last snapshot: begin_epoch=1, snapshot_epoch_id=2");
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_lt_current_ok) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(4); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_eq_current_ok) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(5);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_gt_current_ng) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(6); 
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "end_epoch must be less than or equal to the current epoch id: end_epoch=6, current_epoch_id=5");
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_lt_boot_durable_epoch_ng) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(2); 
    auto status = backend.begin_backup(&request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "end_epoch must be strictly greater than the durable epoch id at boot time: end_epoch=2, boot_durable_epoch_id=3");
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_eq_boot_durable_epoch_ok) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(3); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}

TEST_F(inproc_backend_test, begin_backup_end_epoch_gt_boot_durable_epoch_ok) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    inproc_backend backend(*datastore_, get_location());
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(4); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&request, &response); }, 7);
    EXPECT_TRUE(status.ok());
}


TEST_F(inproc_backend_test, get_wal_history_response_exception_handling) {
    gen_datastore(call_ready_mode::call_ready_manual);
    inproc_backend backend(*datastore_, get_location());
    backend.set_exception_hook([]() { throw std::runtime_error("test exception"); });
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    WalHistoryResponse response;
    auto status = backend.get_wal_history_response(&request, &response);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_STREQ(status.error_message().c_str(), "test exception");
}

TEST_F(inproc_backend_test, begin_backup_exception_handling) {
    gen_datastore(call_ready_mode::call_ready_manual);
    inproc_backend backend(*datastore_, get_location());
    backend.set_exception_hook([]() { throw std::runtime_error("test exception"); });
    BeginBackupRequest request;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(2);
    BeginBackupResponse response;
    auto status = backend.begin_backup(&request, &response);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_STREQ(status.error_message().c_str(), "test exception");
}

TEST_F(inproc_backend_test, begin_and_end_backup_increments_and_decrements_counter) {
    gen_datastore();
    prepare_backup_test_files();
    inproc_backend backend(*datastore_, get_location());

    // Before backup, counter should be 0
    EXPECT_EQ(datastore_->get_impl()->get_backup_counter(), 0);

    // begin_backup
    BeginBackupRequest req;
    req.set_version(begin_backup_message_version);
    req.set_begin_epoch(0);
    req.set_end_epoch(0);
    BeginBackupResponse resp;
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(&req, &resp); }, 7);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(datastore_->get_impl()->get_backup_counter(), 1);

    // end_backup
    limestone::grpc::proto::EndBackupRequest end_req;
    end_req.set_version(limestone::grpc::service::end_backup_message_version);
    end_req.set_session_id(resp.session_id());
    limestone::grpc::proto::EndBackupResponse end_resp;
    status = backend.end_backup(&end_req, &end_resp);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(datastore_->get_impl()->get_backup_counter(), 0);
}

} // namespace limestone::testing
