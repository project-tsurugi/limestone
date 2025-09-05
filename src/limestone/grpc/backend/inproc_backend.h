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
#include "limestone/api/datastore.h"


namespace limestone::grpc::backend {

using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;

class inproc_backend : public grpc_service_backend {
public:
    inproc_backend(limestone::api::datastore& ds, const boost::filesystem::path& log_dir);
    ~inproc_backend() override = default;
    inproc_backend(const inproc_backend&) = delete;
    inproc_backend& operator=(const inproc_backend&) = delete;
    inproc_backend(inproc_backend&&) = delete;
    inproc_backend& operator=(inproc_backend&&) = delete;

    [[nodiscard]] boost::filesystem::path get_log_dir() const noexcept override;

    // gRPC handlers
    ::grpc::Status get_wal_history_response(const limestone::grpc::proto::WalHistoryRequest* request, limestone::grpc::proto::WalHistoryResponse* response) noexcept override;
    ::grpc::Status begin_backup(const BeginBackupRequest* request, BeginBackupResponse* response) noexcept override;
    ::grpc::Status keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept override;
    ::grpc::Status end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept override;
    ::grpc::Status get_object(const limestone::grpc::proto::GetObjectRequest* request, ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept override;

    // For testing: set exception injection hook
    void set_exception_hook(std::function<void()> hook) { exception_hook_ = std::move(hook); }

    // For testing: get backend_shared_impl_
    [[nodiscard]] backend_shared_impl& get_backend_shared_impl() noexcept;
private:
    std::function<void()> exception_hook_;
    limestone::api::datastore& datastore_;
    boost::filesystem::path log_dir_;
    backend_shared_impl backend_shared_impl_;
};

} // namespace limestone::grpc::backend
// Move file to src/limestone/grpc/service/backend/inproc_backend.h
