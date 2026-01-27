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

namespace limestone::grpc::service {

tp_monitor_service_impl::tp_monitor_service_impl(tp_monitor_backend& backend)
    : backend_(backend) {}

tp_monitor_service_impl::~tp_monitor_service_impl() = default;

::grpc::Status tp_monitor_service_impl::Create(::grpc::ServerContext*,
                                               const CreateRequest* request,
                                               CreateResponse* response) {
    auto result = backend_.create(request->txid(), request->tsid());
    response->set_tpmid(result.tpm_id);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Join(::grpc::ServerContext*,
                                             const JoinRequest* request,
                                             JoinResponse* response) {
    auto result = backend_.join(request->tpmid(), request->txid(), request->tsid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::CreateAndJoin(::grpc::ServerContext*,
                                                      const CreateAndJoinRequest* request,
                                                      CreateAndJoinResponse* response) {
    auto result = backend_.create_and_join(request->txid1(),
                                           request->tsid1(),
                                           request->txid2(),
                                           request->tsid2());
    response->set_tpmid(result.tpm_id);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Destroy(::grpc::ServerContext*,
                                                const DestroyRequest* request,
                                                DestroyResponse* response) {
    auto result = backend_.destroy(request->tpmid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Barrier(::grpc::ServerContext*,
                                                const BarrierRequest* request,
                                                BarrierResponse* response) {
    auto result = backend_.barrier_notify(request->tpmid(), request->tsid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
