// backup_service_impl.cpp
// gRPC BackupService スケルトン実装（本体）
#include "backup_service_impl.h"

namespace limestone::grpc::service {

BackupServiceImpl::BackupServiceImpl() = default;
BackupServiceImpl::~BackupServiceImpl() = default;

::grpc::Status BackupServiceImpl::BeginBackup(
    ::grpc::ServerContext* /*context*/,
    const limestone::grpc::backup::BeginBackupRequest* /*request*/,
    limestone::grpc::backup::BeginBackupResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "BeginBackup not implemented"};
}

::grpc::Status BackupServiceImpl::KeepAlive(
    ::grpc::ServerContext* /*context*/,
    const limestone::grpc::backup::KeepAliveRequest* /*request*/,
    limestone::grpc::backup::KeepAliveResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "KeepAlive not implemented"};
}

::grpc::Status BackupServiceImpl::EndBackup(
    ::grpc::ServerContext* /*context*/,
    const limestone::grpc::backup::EndBackupRequest* /*request*/,
    limestone::grpc::backup::EndBackupResponse* /*response*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "EndBackup not implemented"};
}

::grpc::Status BackupServiceImpl::GetObject(
    ::grpc::ServerContext* /*context*/,
    const limestone::grpc::backup::GetObjectRequest* /*request*/,
    ::grpc::ServerWriter<limestone::grpc::backup::GetObjectResponse>* /*writer*/)
{
    // TODO: 実装
    return {::grpc::StatusCode::UNIMPLEMENTED, "GetObject not implemented"};
}

} // namespace limestone::grpc::service
