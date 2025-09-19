#pragma once
#include <grpcpp/grpcpp.h>

#include <memory>

#include "backup.grpc.pb.h"
#include "grpc/backend/grpc_service_backend.h"

namespace limestone::grpc::service {

using limestone::grpc::backend::grpc_service_backend;
using limestone::grpc::proto::BackupService;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::EndBackupRequest;
using limestone::grpc::proto::EndBackupResponse;
using limestone::grpc::proto::GetObjectRequest;
using limestone::grpc::proto::GetObjectResponse;
using limestone::grpc::proto::KeepAliveRequest;
using limestone::grpc::proto::KeepAliveResponse;

class backup_service_impl final : public BackupService::Service {
public:
    explicit backup_service_impl(grpc_service_backend& backend);
    ~backup_service_impl() override;

    backup_service_impl(const backup_service_impl&) = delete;
    backup_service_impl& operator=(const backup_service_impl&) = delete;
    backup_service_impl(backup_service_impl&&) = delete;
    backup_service_impl& operator=(backup_service_impl&&) = delete;
        
    ::grpc::Status BeginBackup(::grpc::ServerContext* context,
                             const BeginBackupRequest* request,
                             BeginBackupResponse* response) override;

    ::grpc::Status KeepAlive(::grpc::ServerContext* context,
                          const KeepAliveRequest* request,
                          KeepAliveResponse* response) override;

    ::grpc::Status EndBackup(::grpc::ServerContext* context,
                          const EndBackupRequest* request,
                          EndBackupResponse* response) override;

    ::grpc::Status GetObject(::grpc::ServerContext* context,
                          const GetObjectRequest* request,
                          ::grpc::ServerWriter<GetObjectResponse>* writer) override;

private:
    grpc_service_backend& backend_;
};

} // namespace limestone::grpc::service
