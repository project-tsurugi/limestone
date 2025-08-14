#ifndef LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
#define LIMESTONE_GRPC_TESTING_PING_SERVICE_H_

#include "ping_service.grpc.pb.h"

namespace limestone::grpc::service {

class ping_service final : public limestone::grpc::proto::PingService::Service {
public:
    ::grpc::Status Ping(::grpc::ServerContext*, const limestone::grpc::proto::PingRequest*, limestone::grpc::proto::PingResponse*) override;
};

} // namespace limestone::grpc::service

#endif // LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
