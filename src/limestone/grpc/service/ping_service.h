#ifndef LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
#define LIMESTONE_GRPC_TESTING_PING_SERVICE_H_

#include "ping_service.grpc.pb.h"

namespace limestone::grpc::service {


using limestone::grpc::proto::PingService;
using limestone::grpc::proto::PingRequest;
using limestone::grpc::proto::PingResponse;

class ping_service final : public PingService::Service {
public:
    ::grpc::Status Ping(::grpc::ServerContext*, const PingRequest*, PingResponse*) override;
};

} // namespace limestone::grpc::service

#endif // LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
