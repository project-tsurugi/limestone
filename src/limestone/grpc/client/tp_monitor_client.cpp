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

tp_monitor_client::result make_result(const limestone::tpmonitor::JoinResponse& response) {
    return tp_monitor_client::result{response.ok(), response.message()};
}

tp_monitor_client::result make_result(const limestone::tpmonitor::DestroyResponse& response) {
    return tp_monitor_client::result{response.ok(), response.message()};
}

tp_monitor_client::result make_result(const limestone::tpmonitor::BarrierNotifyResponse& response) {
    return tp_monitor_client::result{response.ok(), response.message()};
}

} // namespace

tp_monitor_client::tp_monitor_client(std::shared_ptr<::grpc::Channel> channel)
    : stub_(limestone::tpmonitor::TpMonitorService::NewStub(std::move(channel))) {}

tp_monitor_client::create_result tp_monitor_client::create(std::uint32_t participant_count) {
    limestone::tpmonitor::CreateRequest request;
    request.set_participant_count(participant_count);
    limestone::tpmonitor::CreateResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Create(&context, request, &response);
    if (!status.ok()) {
        return create_result{false, 0U, status.error_message()};
    }
    return create_result{response.ok(), response.tpm_id(), response.message()};
}

tp_monitor_client::create_result tp_monitor_client::create_and_join(
        std::uint32_t participant_count,
        const std::vector<std::string>& ts_ids) {
    limestone::tpmonitor::CreateAndJoinRequest request;
    request.set_participant_count(participant_count);
    for (auto const& ts_id : ts_ids) {
        auto* participant = request.add_participants();
        participant->set_ts_id(ts_id);
    }
    limestone::tpmonitor::CreateAndJoinResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->CreateAndJoin(&context, request, &response);
    if (!status.ok()) {
        return create_result{false, 0U, status.error_message()};
    }
    return create_result{response.ok(), response.tpm_id(), response.message()};
}

tp_monitor_client::result tp_monitor_client::join(std::uint64_t tpm_id, std::string_view ts_id) {
    limestone::tpmonitor::JoinRequest request;
    request.set_tpm_id(tpm_id);
    request.mutable_participant()->set_ts_id(std::string(ts_id));
    limestone::tpmonitor::JoinResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Join(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

tp_monitor_client::result tp_monitor_client::destroy(std::uint64_t tpm_id) {
    limestone::tpmonitor::DestroyRequest request;
    request.set_tpm_id(tpm_id);
    limestone::tpmonitor::DestroyResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->Destroy(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

tp_monitor_client::result tp_monitor_client::barrier_notify(
        std::uint64_t tpm_id,
        std::string_view ts_id) {
    limestone::tpmonitor::BarrierNotifyRequest request;
    request.set_tpm_id(tpm_id);
    request.set_ts_id(std::string(ts_id));
    limestone::tpmonitor::BarrierNotifyResponse response;
    ::grpc::ClientContext context;
    auto status = stub_->BarrierNotify(&context, request, &response);
    if (!status.ok()) {
        return result{false, status.error_message()};
    }
    return make_result(response);
}

} // namespace limestone::grpc::client
