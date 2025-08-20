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
#include <memory>
#include <string>
#include <boost/filesystem.hpp>
#include "limestone/api/datastore.h"
#include "wal_sync/wal_history.h"
#include "wal_history.grpc.pb.h"
#include "backup.grpc.pb.h"

namespace limestone::grpc::backend {

using limestone::internal::wal_history;

using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;

// Pure interface for gRPC backends.
// Implementations: inproc_backend, standalone_backend.
class grpc_service_backend {
public:
    virtual ~grpc_service_backend() = default;
    grpc_service_backend(const grpc_service_backend&) = delete;
    grpc_service_backend& operator=(const grpc_service_backend&) = delete;
    grpc_service_backend(grpc_service_backend&&) = delete;
    grpc_service_backend& operator=(grpc_service_backend&&) = delete;

    // Factory helpers (unchanged)
    [[nodiscard]] static std::unique_ptr<grpc_service_backend> create_inproc(limestone::api::datastore& store, const boost::filesystem::path& log_dir);
    [[nodiscard]] static std::unique_ptr<grpc_service_backend> create_standalone(const boost::filesystem::path& log_dir);
    
    // Returns the WAL history response (records and last_epoch) as defined in .proto
    virtual ::grpc::Status get_wal_history_response(const WalHistoryRequest* request, WalHistoryResponse* response) noexcept = 0;

   virtual ::grpc::Status begin_backup(BeginBackupRequest* request, BeginBackupResponse* response) noexcept = 0;

    // Returns the log directory path (for debugging purposes)
    [[nodiscard]] virtual boost::filesystem::path get_log_dir() const noexcept = 0;
protected:
    grpc_service_backend() = default;
};

} // namespace limestone::grpc::backend
