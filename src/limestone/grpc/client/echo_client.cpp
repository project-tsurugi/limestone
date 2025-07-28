#include "echo_client.h"
#include <glog/logging.h>
#include <chrono>

namespace limestone::grpc::client {

echo_client::echo_client(const std::string& server_address) 
    : stub_(limestone::grpc::EchoService::NewStub(
        ::grpc::CreateChannel(server_address, ::grpc::InsecureChannelCredentials()))) {
    LOG(INFO) << "echo_client created for server: " << server_address;
}

echo_client::echo_client(std::shared_ptr<::grpc::Channel> channel)
    : stub_(limestone::grpc::EchoService::NewStub(channel)) {
    LOG(INFO) << "echo_client created with custom channel";
}

::grpc::Status echo_client::echo(const std::string& message, std::string& response) {
    limestone::grpc::EchoRequest request;
    limestone::grpc::EchoResponse echo_response;
    ::grpc::ClientContext context;
    
    request.set_message(message);
    
    LOG(INFO) << "Sending echo request: " << message;
    
    ::grpc::Status status = stub_->Echo(&context, request, &echo_response);
    
    if (status.ok()) {
        response = echo_response.message();
        LOG(INFO) << "Echo response received: " << response;
    } else {
        LOG(ERROR) << "Echo RPC failed: " << status.error_code() 
                   << ": " << status.error_message();
    }
    
    return status;
}

::grpc::Status echo_client::echo(const std::string& message, 
                             std::string& response,
                             int timeout_ms) {
    limestone::grpc::EchoRequest request;
    limestone::grpc::EchoResponse echo_response;
    ::grpc::ClientContext context;
    
    // Set timeout
    auto deadline = std::chrono::system_clock::now() + 
                   std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    
    request.set_message(message);
    
    LOG(INFO) << "Sending echo request with timeout " << timeout_ms 
              << "ms: " << message;
    
    ::grpc::Status status = stub_->Echo(&context, request, &echo_response);
    
    if (status.ok()) {
        response = echo_response.message();
        LOG(INFO) << "Echo response received: " << response;
    } else {
        LOG(ERROR) << "Echo RPC failed: " << status.error_code() 
                   << ": " << status.error_message();
    }
    
    return status;
}

} // namespace limestone::grpc::client
