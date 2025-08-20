#pragma once
#include <memory>
#include <grpcpp/grpcpp.h>
#include "backup.grpc.pb.h"

namespace limestone::grpc::service {

using limestone::grpc::proto::BackupService;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::KeepAliveRequest;
using limestone::grpc::proto::KeepAliveResponse;
using limestone::grpc::proto::EndBackupRequest;
using limestone::grpc::proto::EndBackupResponse;
using limestone::grpc::proto::GetObjectRequest;
using limestone::grpc::proto::GetObjectResponse;

class BackupServiceImpl final : public BackupService::Service {
public:
    BackupServiceImpl();
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
};

} // namespace limestone::grpc::service
