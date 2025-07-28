#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "echo.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using echo::EchoRequest;
using echo::EchoReply;
using echo::EchoService;

int main(int argc, char** argv) {
  std::string target_str = "localhost:50051";
  std::string message = "Hello, gRPC!";
  
  if (argc > 1) {
    message = argv[1];
  }

// Message size check
if (message.size() > 100) {
    std::cerr << "Error: Message too long (max 100 chars)" << std::endl;
    return 1;
}

// Create a simple channel
auto channel = grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());
auto stub = EchoService::NewStub(channel);

// Request/Response
EchoRequest request;
EchoReply reply;
ClientContext context;

request.set_message(message);

// gRPC call
  Status status = stub->Echo(&context, request, &reply);

  if (status.ok()) {
    std::cout << "Server replied: " << reply.message() << std::endl;
  } else {
    std::cout << "RPC failed: " << status.error_message() << std::endl;
  }

  return 0;
}
