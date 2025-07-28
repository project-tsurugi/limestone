#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "echo_service.grpc.pb.h"

namespace limestone::grpc::client {

/**
 * @brief Echo service client for testing and demonstration
 * 
 * This client provides a convenient interface to communicate with
 * the echo_service over gRPC.
 */
class echo_client {
public:
    /**
     * @brief Construct echo_client with server address
     * 
     * @param server_address Server address in format "host:port"
     */
    explicit echo_client(const std::string& server_address);
    
    /**
     * @brief Constructor with custom gRPC channel
     * @param channel Shared pointer to gRPC channel
     */
    explicit echo_client(std::shared_ptr<::grpc::Channel> channel);
    
    /**
     * @brief Send echo request and receive response
     * 
     * @param message Message to be echoed
     * @param response Output parameter to store the echoed message
     * @return grpc::Status indicating success or failure
     */
    /**
     * @brief Send an echo request to the server
     * @param message Message to echo
     * @param response Reference to store the response
     * @return gRPC status of the operation
     */
    ::grpc::Status echo(const std::string& message, std::string& response);
    
    /**
     * @brief Send echo request with timeout
     * 
     * @param message Message to be echoed
     * @param response Output parameter to store the echoed message
     * @param timeout_ms Timeout in milliseconds
     * @return grpc::Status indicating success or failure
     */
    /**
     * @brief Send an echo request with timeout
     * @param message Message to echo
     * @param response Reference to store the response
     * @param timeout_ms Timeout in milliseconds (default: 1000ms)
     * @return gRPC status of the operation
     */
    ::grpc::Status echo(const std::string& message,
                        std::string& response,
                        int timeout_ms = 1000);

private:
    std::unique_ptr<limestone::grpc::EchoService::Stub> stub_;
};

} // namespace limestone::grpc::client
