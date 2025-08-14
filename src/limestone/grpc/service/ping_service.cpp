#include "ping_service.h"

namespace limestone::grpc::service {

::grpc::Status ping_service::Ping(::grpc::ServerContext* /* context */, const limestone::grpc::proto::PingRequest* request,
                                  limestone::grpc::proto::PingResponse* response) {
    (void)request;
    (void)response;
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
