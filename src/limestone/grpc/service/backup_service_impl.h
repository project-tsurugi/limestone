#pragma once
#include <memory>
#include <grpcpp/grpcpp.h>
#include "backup.grpc.pb.h"

namespace limestone::grpc::service {

using BackupService = limestone::grpc::proto::BackupService;
using BeginBackupRequest = limestone::grpc::proto::BeginBackupRequest;
using BeginBackupResponse = limestone::grpc::proto::BeginBackupResponse;
using KeepAliveRequest = limestone::grpc::proto::KeepAliveRequest;
using KeepAliveResponse = limestone::grpc::proto::KeepAliveResponse;
using EndBackupRequest = limestone::grpc::proto::EndBackupRequest;
using EndBackupResponse = limestone::grpc::proto::EndBackupResponse;
using GetObjectRequest = limestone::grpc::proto::GetObjectRequest;
using GetObjectResponse = limestone::grpc::proto::GetObjectResponse;

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
