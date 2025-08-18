#pragma once
#include <memory>
#include <grpcpp/grpcpp.h>
#include "backup.grpc.pb.h"

namespace limestone::grpc::service {

class BackupServiceImpl final : public limestone::grpc::backup::BackupService::Service {
public:
    BackupServiceImpl();
    ~BackupServiceImpl() override;

    BackupServiceImpl(const BackupServiceImpl&) = delete;
    BackupServiceImpl& operator=(const BackupServiceImpl&) = delete;
    BackupServiceImpl(BackupServiceImpl&&) = delete;
    BackupServiceImpl& operator=(BackupServiceImpl&&) = delete;
        
    ::grpc::Status BeginBackup(::grpc::ServerContext* context,
                             const limestone::grpc::backup::BeginBackupRequest* request,
                             limestone::grpc::backup::BeginBackupResponse* response) override;

    ::grpc::Status KeepAlive(::grpc::ServerContext* context,
                          const limestone::grpc::backup::KeepAliveRequest* request,
                          limestone::grpc::backup::KeepAliveResponse* response) override;

    ::grpc::Status EndBackup(::grpc::ServerContext* context,
                          const limestone::grpc::backup::EndBackupRequest* request,
                          limestone::grpc::backup::EndBackupResponse* response) override;

    ::grpc::Status GetObject(::grpc::ServerContext* context,
                          const limestone::grpc::backup::GetObjectRequest* request,
                          ::grpc::ServerWriter<limestone::grpc::backup::GetObjectResponse>* writer) override;
};

} // namespace limestone::grpc::service
