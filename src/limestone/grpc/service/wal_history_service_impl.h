#pragma once

#include <grpcpp/grpcpp.h>
#include "grpc/backend/grpc_service_backend.h"
#include "wal_history.grpc.pb.h"

namespace limestone::grpc::service {

using limestone::grpc::backend::grpc_service_backend;
using limestone::grpc::proto::WalHistoryService;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;

class wal_history_service_impl final : public WalHistoryService::Service {
public:
    explicit wal_history_service_impl(grpc_service_backend& backend);
    ~wal_history_service_impl() override = default;
    wal_history_service_impl(const wal_history_service_impl&) = delete;
    wal_history_service_impl& operator=(const wal_history_service_impl&) = delete;
    wal_history_service_impl(wal_history_service_impl&&) = delete;
    wal_history_service_impl& operator=(wal_history_service_impl&&) = delete;

    ::grpc::Status GetWalHistory(
        ::grpc::ServerContext* context,
        const WalHistoryRequest* request,
        WalHistoryResponse* response) override;

private:
    grpc_service_backend& backend_;
};

} // namespace limestone::grpc::service
