// backup_service_impl.cpp
// gRPC BackupService スケルトン実装（本体）
#include "backup_service_impl.h"

namespace limestone::grpc::service {

// Type aliases for proto types used in this translation unit.
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::KeepAliveRequest;
using limestone::grpc::proto::KeepAliveResponse;
using limestone::grpc::proto::EndBackupRequest;
using limestone::grpc::proto::EndBackupResponse;
using limestone::grpc::proto::GetObjectRequest;
using limestone::grpc::proto::GetObjectResponse;


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
