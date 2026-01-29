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
#include <grpc/client/tp_monitor_client.h>

#include <string>
#include <utility>

namespace limestone::grpc::client {

namespace {

tp_monitor_client::result make_result(const disttx::grpc::proto::JoinResponse& response) {
    return tp_monitor_client::result{response.success(), ""};
}

tp_monitor_client::result make_result(const disttx::grpc::proto::DestroyResponse& response) {
    return tp_monitor_client::result{response.success(), ""};
}

tp_monitor_client::result make_result(const disttx::grpc::proto::BarrierResponse& response) {
    return tp_monitor_client::result{response.success(), ""};
}

} // namespace

tp_monitor_client::tp_monitor_client(std::shared_ptr<::grpc::Channel> channel)
    : stub_(disttx::grpc::proto::TpMonitorService::NewStub(std::move(channel))) {}

tp_monitor_client::create_result tp_monitor_client::create() {
    disttx::grpc::proto::CreateRequest request;
    disttx::grpc::proto::CreateResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Create(&context, request, &response);
    if (!status.ok()) {
        return create_result{false, 0U, status.error_message()};
    }
    return create_result{true, response.tpmid(), ""};
}

tp_monitor_client::create_result tp_monitor_client::create_and_join(
        std::string_view tx_id1,
        std::uint64_t ts_id1,
        std::string_view tx_id2,
        std::uint64_t ts_id2) {
    disttx::grpc::proto::CreateAndJoinRequest request;
    request.set_txid1(std::string(tx_id1));
    request.set_tsid1(ts_id1);
    request.set_txid2(std::string(tx_id2));
    request.set_tsid2(ts_id2);
    disttx::grpc::proto::CreateAndJoinResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->CreateAndJoin(&context, request, &response);
    if (!status.ok()) {
        return create_result{false, 0U, status.error_message()};
    }
    return create_result{true, response.tpmid(), ""};
}

tp_monitor_client::result tp_monitor_client::join(std::uint64_t tpm_id,
                                                  std::string_view tx_id,
                                                  std::uint64_t ts_id) {
    disttx::grpc::proto::JoinRequest request;
    request.set_tpmid(tpm_id);
    request.set_txid(std::string(tx_id));
    request.set_tsid(ts_id);
    disttx::grpc::proto::JoinResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Join(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

tp_monitor_client::result tp_monitor_client::destroy(std::uint64_t tpm_id) {
    disttx::grpc::proto::DestroyRequest request;
    request.set_tpmid(tpm_id);
    disttx::grpc::proto::DestroyResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Destroy(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

tp_monitor_client::result tp_monitor_client::barrier_notify(
        std::uint64_t tpm_id,
        std::string_view tx_id) {
    disttx::grpc::proto::BarrierRequest request;
    request.set_tpmid(tpm_id);
    request.set_txid(std::string(tx_id));
    disttx::grpc::proto::BarrierResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Barrier(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

} // namespace limestone::grpc::client
