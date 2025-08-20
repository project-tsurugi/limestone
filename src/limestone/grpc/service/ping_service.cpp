#include "ping_service.h"

namespace limestone::grpc::service {

using limestone::grpc::proto::PingRequest;
using limestone::grpc::proto::PingResponse;

::grpc::Status ping_service::Ping(::grpc::ServerContext* /* context */, const limestone::grpc::proto::PingRequest* request,
                                  PingResponse* response) {
    (void)request;
    (void)response;
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
