#ifndef LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
#define LIMESTONE_GRPC_TESTING_PING_SERVICE_H_

#include "ping_service.grpc.pb.h"

namespace limestone::grpc::testing {

class ping_service final : public PingService::Service {
public:
    ::grpc::Status Ping(::grpc::ServerContext*, const PingRequest*, PingResponse*) override {
        return ::grpc::Status::OK;
    }
};

} // namespace limestone::grpc::testing

#endif // LIMESTONE_GRPC_TESTING_PING_SERVICE_H_
