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
#pragma once

#include <vector>
#include "grpc_service_backend.h"
#include "backend_shared_impl.h"

namespace limestone::grpc::backend {

using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;

class standalone_backend : public grpc_service_backend {
public:
    explicit standalone_backend(const boost::filesystem::path& log_dir);
    ~standalone_backend() override = default;

    standalone_backend(const standalone_backend&) = delete;
    standalone_backend& operator=(const standalone_backend&) = delete;
    standalone_backend(standalone_backend&&) = delete;
    standalone_backend& operator=(standalone_backend&&) = delete;

    [[nodiscard]] boost::filesystem::path get_log_dir() const noexcept override;
    ::grpc::Status get_wal_history_response(const limestone::grpc::proto::WalHistoryRequest* request, limestone::grpc::proto::WalHistoryResponse* response) noexcept override;
    ::grpc::Status begin_backup(const limestone::grpc::proto::BeginBackupRequest* request, limestone::grpc::proto::BeginBackupResponse* response) noexcept override;
    ::grpc::Status keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept override;
    ::grpc::Status end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept override;
    ::grpc::Status get_object(const limestone::grpc::proto::GetObjectRequest* request, ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept override;
private:
    boost::filesystem::path log_dir_;
    backend_shared_impl backend_shared_impl_;
    limestone::api::datastore datastore_;
};

} // namespace limestone::grpc::backend
