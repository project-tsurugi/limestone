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

class BackupServiceImpl final : public BackupService::Service {
public:
    explicit BackupServiceImpl(grpc_service_backend& backend);
    ~BackupServiceImpl() override;

    BackupServiceImpl(const BackupServiceImpl&) = delete;
    BackupServiceImpl& operator=(const BackupServiceImpl&) = delete;
    BackupServiceImpl(BackupServiceImpl&&) = delete;
    BackupServiceImpl& operator=(BackupServiceImpl&&) = delete;
        
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
