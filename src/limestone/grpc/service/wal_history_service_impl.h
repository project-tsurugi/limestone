#pragma once

#include <grpcpp/grpcpp.h>
#include "grpc/backend/grpc_service_backend.h"
#include "wal_history.grpc.pb.h"

namespace limestone::grpc::service {

using namespace limestone::grpc::backend;

class wal_history_service_impl final : public limestone::grpc::proto::WalHistoryService::Service {
public:
    explicit wal_history_service_impl(grpc_service_backend& backend);
    ~wal_history_service_impl() override = default;
    wal_history_service_impl(const wal_history_service_impl&) = delete;
    wal_history_service_impl& operator=(const wal_history_service_impl&) = delete;
    wal_history_service_impl(wal_history_service_impl&&) = delete;
    wal_history_service_impl& operator=(wal_history_service_impl&&) = delete;

    ::grpc::Status GetWalHistory(
        ::grpc::ServerContext* context,
        const limestone::grpc::proto::WalHistoryRequest* request,
        limestone::grpc::proto::WalHistoryResponse* response) override;

private:
    grpc_service_backend& backend_;
};

} // namespace limestone::grpc::service
