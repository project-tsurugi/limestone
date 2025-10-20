#include "backup_service_impl.h"
#include <glog/logging.h>
#include "limestone/logging.h"
#include "logging_helper.h"

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


backup_service_impl::backup_service_impl(grpc_service_backend& backend) : backend_(backend) {}
backup_service_impl::~backup_service_impl() = default;

::grpc::Status backup_service_impl::BeginBackup(
    ::grpc::ServerContext* /*context*/,
    const BeginBackupRequest* request,
    BeginBackupResponse* response)
{
    VLOG_LP(log_info) << "BeginBackup called";
    auto status = backend_.begin_backup(request, response);
    VLOG_LP(log_info) << "BeginBackup status: " << status.error_code() << " " << status.error_message();
    return status;
}

::grpc::Status backup_service_impl::KeepAlive(
    ::grpc::ServerContext* /*context*/,
    const KeepAliveRequest* request,
    KeepAliveResponse* response)
{
    VLOG_LP(log_info) << "KeepAlive called";
    return backend_.keep_alive(request, response);
}

::grpc::Status backup_service_impl::EndBackup(
    ::grpc::ServerContext* /*context*/,
    const EndBackupRequest* request,
    EndBackupResponse* response)
{
    VLOG_LP(log_info) << "EndBackup called";
    return backend_.end_backup(request, response);
}

::grpc::Status backup_service_impl::GetObject(
    ::grpc::ServerContext* /*context*/,
    const GetObjectRequest* request,
    ::grpc::ServerWriter<GetObjectResponse>* writer)
{
    VLOG_LP(log_info) << "GetObject called";
    return backend_.get_object(request, writer);
}

} // namespace limestone::grpc::service
