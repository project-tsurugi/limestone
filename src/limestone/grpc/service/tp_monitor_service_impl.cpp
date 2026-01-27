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
#include <grpc/service/tp_monitor_service_impl.h>

#include <string>
#include <vector>

namespace limestone::grpc::service {

tp_monitor_service_impl::tp_monitor_service_impl(tp_monitor_backend& backend)
    : backend_(backend) {}

tp_monitor_service_impl::~tp_monitor_service_impl() = default;

::grpc::Status tp_monitor_service_impl::Create(::grpc::ServerContext*,
                                               const CreateRequest* request,
                                               CreateResponse* response) {
    auto result = backend_.create(request->participant_count());
    response->set_ok(result.ok);
    response->set_tpm_id(result.tpm_id);
    response->set_message(result.message);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Join(::grpc::ServerContext*,
                                             const JoinRequest* request,
                                             JoinResponse* response) {
    const auto& participant = request->participant();
    auto result = backend_.join(request->tpm_id(), participant.ts_id());
    response->set_ok(result.ok);
    response->set_message(result.message);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::CreateAndJoin(::grpc::ServerContext*,
                                                      const CreateAndJoinRequest* request,
                                                      CreateAndJoinResponse* response) {
    std::vector<std::string> participants{};
    participants.reserve(static_cast<std::size_t>(request->participants_size()));
    for (const auto& participant : request->participants()) {
        participants.emplace_back(participant.ts_id());
    }
    auto result = backend_.create_and_join(request->participant_count(), participants);
    response->set_ok(result.ok);
    response->set_tpm_id(result.tpm_id);
    response->set_message(result.message);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Destroy(::grpc::ServerContext*,
                                                const DestroyRequest* request,
                                                DestroyResponse* response) {
    auto result = backend_.destroy(request->tpm_id());
    response->set_ok(result.ok);
    response->set_message(result.message);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::BarrierNotify(::grpc::ServerContext*,
                                                      const BarrierNotifyRequest* request,
                                                      BarrierNotifyResponse* response) {
    auto result = backend_.barrier_notify(request->tpm_id(), request->ts_id());
    response->set_ok(result.ok);
    response->set_message(result.message);
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
