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

class tp_monitor_service_impl_testable : public limestone::grpc::service::tp_monitor_service_impl {
public:
    using limestone::grpc::service::tp_monitor_service_impl::barrier_notify_monitor;
    using limestone::grpc::service::tp_monitor_service_impl::destroy_monitor;
    using limestone::grpc::service::tp_monitor_service_impl::join_monitor;
};

TEST_F(tp_monitor_service_impl_test, create_returns_tpm_id) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest request{};
    disttx::grpc::proto::CreateResponse response{};
    ::grpc::ServerContext context{};
    request.set_participantcount(2U);
    auto status = service.Create(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.tpmid() != 0U);
}

TEST_F(tp_monitor_service_impl_test, join_duplicate_tx_id_is_ignored) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(2U);
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
    EXPECT_TRUE(join_response.success());

    disttx::grpc::proto::JoinRequest duplicate_request{};
    disttx::grpc::proto::JoinResponse duplicate_response{};
    ::grpc::ServerContext duplicate_context{};
    duplicate_request.set_tpmid(create_response.tpmid());
    duplicate_request.set_txid("tx-1");
    duplicate_request.set_tsid(2U);
    auto duplicate_status = service.Join(&duplicate_context, &duplicate_request, &duplicate_response);
    EXPECT_TRUE(duplicate_status.ok());
    EXPECT_TRUE(! duplicate_response.success());
}

TEST_F(tp_monitor_service_impl_test, barrier_notify_requires_join) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(2U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    disttx::grpc::proto::BarrierRequest pre_notify_request{};
    disttx::grpc::proto::BarrierResponse pre_notify_response{};
    ::grpc::ServerContext pre_notify_context{};
    pre_notify_request.set_tpmid(create_response.tpmid());
    pre_notify_request.set_txid("tx-2");
    auto pre_notify_status = service.Barrier(&pre_notify_context,
                                             &pre_notify_request,
                                             &pre_notify_response);
    EXPECT_TRUE(pre_notify_status.ok());
    EXPECT_TRUE(! pre_notify_response.success());

    disttx::grpc::proto::JoinRequest join_request{};
    disttx::grpc::proto::JoinResponse join_response{};
    ::grpc::ServerContext join_context{};
    join_request.set_tpmid(create_response.tpmid());
    join_request.set_txid("tx-1");
    join_request.set_tsid(1U);
    auto join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    join_request.set_txid("tx-2");
    join_request.set_tsid(2U);
    join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    auto first_notify_future = std::async(std::launch::async, [&service, &create_response]() {
        disttx::grpc::proto::BarrierRequest request{};
        disttx::grpc::proto::BarrierResponse response{};
        ::grpc::ServerContext context{};
        request.set_tpmid(create_response.tpmid());
        request.set_txid("tx-1");
        auto status = service.Barrier(&context, &request, &response);
        EXPECT_TRUE(status.ok());
        return response;
    });

    disttx::grpc::proto::BarrierRequest notify_request{};
    disttx::grpc::proto::BarrierResponse notify_response{};
    ::grpc::ServerContext notify_context{};
    notify_request.set_tpmid(create_response.tpmid());
    notify_request.set_txid("tx-2");
    auto notify_status = service.Barrier(&notify_context, &notify_request, &notify_response);
    EXPECT_TRUE(notify_status.ok());
    EXPECT_TRUE(notify_response.success());
    EXPECT_TRUE(first_notify_future.get().success());
}

TEST_F(tp_monitor_service_impl_test, create_respects_participant_count) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(3U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    disttx::grpc::proto::JoinRequest join_request{};
    disttx::grpc::proto::JoinResponse join_response{};
    ::grpc::ServerContext join_context{};
    join_request.set_tpmid(create_response.tpmid());
    join_request.set_txid("tx-1");
    join_request.set_tsid(1U);
    auto join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    join_request.set_txid("tx-2");
    join_request.set_tsid(2U);
    join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    join_request.set_txid("tx-3");
    join_request.set_tsid(3U);
    join_status = service.Join(&join_context, &join_request, &join_response);
    ASSERT_TRUE(join_status.ok());
    ASSERT_TRUE(join_response.success());

    auto first_notify_future = std::async(std::launch::async, [&service, &create_response]() {
        disttx::grpc::proto::BarrierRequest request{};
        disttx::grpc::proto::BarrierResponse response{};
        ::grpc::ServerContext context{};
        request.set_tpmid(create_response.tpmid());
        request.set_txid("tx-1");
        auto status = service.Barrier(&context, &request, &response);
        EXPECT_TRUE(status.ok());
        return response;
    });

    auto second_notify_future = std::async(std::launch::async, [&service, &create_response]() {
        disttx::grpc::proto::BarrierRequest request{};
        disttx::grpc::proto::BarrierResponse response{};
        ::grpc::ServerContext context{};
        request.set_tpmid(create_response.tpmid());
        request.set_txid("tx-2");
        auto status = service.Barrier(&context, &request, &response);
        EXPECT_TRUE(status.ok());
        return response;
    });

    disttx::grpc::proto::BarrierRequest notify_request{};
    disttx::grpc::proto::BarrierResponse notify_response{};
    ::grpc::ServerContext notify_context{};
    notify_request.set_tpmid(create_response.tpmid());
    notify_request.set_txid("tx-3");
    auto notify_status = service.Barrier(&notify_context, &notify_request, &notify_response);
    EXPECT_TRUE(notify_status.ok());
    EXPECT_TRUE(notify_response.success());
    EXPECT_TRUE(first_notify_future.get().success());
    EXPECT_TRUE(second_notify_future.get().success());
}

TEST_F(tp_monitor_service_impl_test, destroy_removes_monitor_state) { // NOLINT
    limestone::grpc::service::tp_monitor_service_impl service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(2U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    disttx::grpc::proto::DestroyRequest destroy_request{};
    disttx::grpc::proto::DestroyResponse destroy_response{};
    ::grpc::ServerContext destroy_context{};
    destroy_request.set_tpmid(create_response.tpmid());
    auto destroy_status = service.Destroy(&destroy_context, &destroy_request, &destroy_response);
    EXPECT_TRUE(destroy_status.ok());
    EXPECT_TRUE(destroy_response.success());

    disttx::grpc::proto::JoinRequest join_request{};
    disttx::grpc::proto::JoinResponse join_response{};
    ::grpc::ServerContext join_context{};
    join_request.set_tpmid(create_response.tpmid());
    join_request.set_txid("tx-2");
    join_request.set_tsid(2U);
    auto join_status = service.Join(&join_context, &join_request, &join_response);
    EXPECT_TRUE(join_status.ok());
    EXPECT_TRUE(! join_response.success());
}

TEST_F(tp_monitor_service_impl_test, join_monitor_requires_existing_monitor) { // NOLINT
    tp_monitor_service_impl_testable service{};
    auto result = service.join_monitor(123U, "tx-1", 1U);
    EXPECT_TRUE(! result.ok);
}

TEST_F(tp_monitor_service_impl_test, barrier_notify_monitor_requires_join) { // NOLINT
    tp_monitor_service_impl_testable service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(2U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    auto pre_notify = service.barrier_notify_monitor(create_response.tpmid(), "tx-2");
    EXPECT_TRUE(! pre_notify.ok);

    auto join_result1 = service.join_monitor(create_response.tpmid(), "tx-1", 1U);
    ASSERT_TRUE(join_result1.ok);
    auto join_result2 = service.join_monitor(create_response.tpmid(), "tx-2", 2U);
    ASSERT_TRUE(join_result2.ok);

    auto first_notify_future = std::async(std::launch::async, [&service, &create_response]() {
        return service.barrier_notify_monitor(create_response.tpmid(), "tx-1");
    });
    auto notify_result = service.barrier_notify_monitor(create_response.tpmid(), "tx-2");
    EXPECT_TRUE(notify_result.ok);
    EXPECT_TRUE(first_notify_future.get().ok);
}

TEST_F(tp_monitor_service_impl_test, destroy_monitor_removes_state) { // NOLINT
    tp_monitor_service_impl_testable service{};
    disttx::grpc::proto::CreateRequest create_request{};
    disttx::grpc::proto::CreateResponse create_response{};
    ::grpc::ServerContext create_context{};
    create_request.set_participantcount(2U);
    auto create_status = service.Create(&create_context, &create_request, &create_response);
    ASSERT_TRUE(create_status.ok());

    auto destroy_result = service.destroy_monitor(create_response.tpmid());
    EXPECT_TRUE(destroy_result.ok);

    auto join_result = service.join_monitor(create_response.tpmid(), "tx-2", 2U);
    EXPECT_TRUE(! join_result.ok);
}

} // namespace limestone::testing
