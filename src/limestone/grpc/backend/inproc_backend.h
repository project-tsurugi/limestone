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
    ::grpc::Status get_wal_history_response(const limestone::grpc::proto::WalHistoryRequest* request, limestone::grpc::proto::WalHistoryResponse* response) noexcept override;
    ::grpc::Status begin_backup(BeginBackupRequest* request, BeginBackupResponse* response) noexcept override;
private:
    limestone::api::datastore& datastore_;
    boost::filesystem::path log_dir_;
    backend_shared_impl backend_shared_impl_;
};

} // namespace limestone::grpc::backend
// Move file to src/limestone/grpc/service/backend/inproc_backend.h
