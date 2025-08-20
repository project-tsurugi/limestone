// backup_service_impl.cpp
// gRPC BackupService スケルトン実装（本体）
#include "backup_service_impl.h"

namespace limestone::grpc::service {

// Type aliases for proto types used in this translation unit.
using BeginBackupRequest = limestone::grpc::proto::BeginBackupRequest;
using BeginBackupResponse = limestone::grpc::proto::BeginBackupResponse;
using KeepAliveRequest = limestone::grpc::proto::KeepAliveRequest;
using KeepAliveResponse = limestone::grpc::proto::KeepAliveResponse;
using EndBackupRequest = limestone::grpc::proto::EndBackupRequest;
using EndBackupResponse = limestone::grpc::proto::EndBackupResponse;
using GetObjectRequest = limestone::grpc::proto::GetObjectRequest;
using GetObjectResponse = limestone::grpc::proto::GetObjectResponse;


BackupServiceImpl::BackupServiceImpl() = default;
BackupServiceImpl::~BackupServiceImpl() = default;

::grpc::Status BackupServiceImpl::BeginBackup(
    ::grpc::ServerContext* /*context*/,
    const BeginBackupRequest* /*request*/,
    BeginBackupResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "BeginBackup not implemented"};
}

::grpc::Status BackupServiceImpl::KeepAlive(
    ::grpc::ServerContext* /*context*/,
    const KeepAliveRequest* /*request*/,
    KeepAliveResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "KeepAlive not implemented"};
}

::grpc::Status BackupServiceImpl::EndBackup(
    ::grpc::ServerContext* /*context*/,
    const EndBackupRequest* /*request*/,
    EndBackupResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "EndBackup not implemented"};
}

::grpc::Status BackupServiceImpl::GetObject(
    ::grpc::ServerContext* /*context*/,
    const GetObjectRequest* /*request*/,
    ::grpc::ServerWriter<GetObjectResponse>* /*writer*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "GetObject not implemented"};
}

} // namespace limestone::grpc::service
