/*
 * Copyright 2026 Project Tsurugi.
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

#include <future>
#include <string>

#include <grpc/service/tp_monitor_service_impl.h>
#include <test_root.h>

namespace limestone::testing {

class tp_monitor_service_impl_test : public ::testing::Test {};

TEST_F(tp_monitor_service_impl_test, create_returns_tpm_id) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest request{};
    disttx::grpc::proto::CreateResponse response{};
    ::grpc::ServerContext context{};
    request.set_txid("tx-1");
    request.set_tsid(1U);
    auto status = service.Create(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.tpmid() != 0U);
}

TEST_F(tp_monitor_service_impl_test, join_duplicate_ts_id_is_ignored) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_txid("tx-1");
    create_request.set_tsid(1U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    disttx::grpc::proto::JoinRequest join_request{};
    disttx::grpc::proto::JoinResponse join_response{};
    ::grpc::ServerContext join_context{};
    join_request.set_tpmid(create_response.tpmid());
    join_request.set_txid("tx-1");
    join_request.set_tsid(1U);
    auto join_status = service.Join(&join_context, &join_request, &join_response);
    EXPECT_TRUE(join_status.ok());
    EXPECT_TRUE(! join_response.success());
}

TEST_F(tp_monitor_service_impl_test, barrier_notify_requires_join) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_txid("tx-1");
    create_request.set_tsid(1U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    disttx::grpc::proto::BarrierRequest pre_notify_request{};
    disttx::grpc::proto::BarrierResponse pre_notify_response{};
    ::grpc::ServerContext pre_notify_context{};
    pre_notify_request.set_tpmid(create_response.tpmid());
    pre_notify_request.set_tsid(2U);
    auto pre_notify_status = service.Barrier(&pre_notify_context,
                                             &pre_notify_request,
                                             &pre_notify_response);
    EXPECT_TRUE(pre_notify_status.ok());
    EXPECT_TRUE(! pre_notify_response.success());

    disttx::grpc::proto::JoinRequest join_request{};
    disttx::grpc::proto::JoinResponse join_response{};
    ::grpc::ServerContext join_context{};
    join_request.set_tpmid(create_response.tpmid());
    join_request.set_txid("tx-2");
    join_request.set_tsid(2U);
    auto join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    auto first_notify_future = std::async(std::launch::async, [&service, &create_response]() {
        disttx::grpc::proto::BarrierRequest request{};
        disttx::grpc::proto::BarrierResponse response{};
        ::grpc::ServerContext context{};
        request.set_tpmid(create_response.tpmid());
        request.set_tsid(1U);
        auto status = service.Barrier(&context, &request, &response);
        EXPECT_TRUE(status.ok());
        return response;
    });

    disttx::grpc::proto::BarrierRequest notify_request{};
    disttx::grpc::proto::BarrierResponse notify_response{};
    ::grpc::ServerContext notify_context{};
    notify_request.set_tpmid(create_response.tpmid());
    notify_request.set_tsid(2U);
    auto notify_status = service.Barrier(&notify_context, &notify_request, &notify_response);
    EXPECT_TRUE(notify_status.ok());
    EXPECT_TRUE(notify_response.success());
    EXPECT_TRUE(first_notify_future.get().success());
}

} // namespace limestone::testing
