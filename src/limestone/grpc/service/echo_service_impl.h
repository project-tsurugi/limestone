#pragma once

#include <grpcpp/grpcpp.h>
#include "echo_service.grpc.pb.h"

namespace limestone::grpc::service {

/**
 * @brief Echo service implementation for testing and demonstration
 * 
 * This service provides a simple echo functionality that returns
 * the input message back to the client.
 */
class echo_service_impl final : public limestone::grpc::proto::EchoService::Service {
public:
    /**
     * @brief Echo RPC implementation
     * @param context Server context for the RPC call
     * @param request Echo request containing the message
     * @param response Echo response to be filled
     * @return Status of the RPC operation
     */
    ::grpc::Status Echo(::grpc::ServerContext* context,
                        const limestone::grpc::proto::EchoRequest* request,
                        limestone::grpc::proto::EchoResponse* response) override;
};

} // namespace limestone::grpc::service
