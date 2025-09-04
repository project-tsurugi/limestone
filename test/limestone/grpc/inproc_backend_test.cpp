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

using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::list_wal_history_message_version;
using limestone::internal::blob_file_resolver;
using limestone::internal::wal_history;
using limestone::grpc::service::session_timeout_seconds;






class inproc_backend_test : public backend_test_fixture {
protected:

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
