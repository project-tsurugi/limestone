
#include "wal_history_service_impl.h"
#include <glog/logging.h>
#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::grpc::service {

wal_history_service_impl::wal_history_service_impl(grpc_service_backend& backend)
    : backend_(backend) {}


::grpc::Status wal_history_service_impl::GetWalHistory(
    ::grpc::ServerContext* /* context */,
    const limestone::grpc::proto::WalHistoryRequest* /* request */,
    limestone::grpc::proto::WalHistoryResponse* response) {
    VLOG_LP(log_info) << "GetWalHistory called";
    return backend_.get_wal_history_response(response);
}

} // namespace limestone::grpc::service
