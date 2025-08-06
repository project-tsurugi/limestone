#include "ping_service.h"

namespace limestone::grpc::testing {

::grpc::Status ping_service::Ping(::grpc::ServerContext* /* context */, const PingRequest* request, PingResponse* response) {
    (void)request;
    (void)response;
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::testing
