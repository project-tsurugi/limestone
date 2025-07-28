#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include "limestone/grpc/service/echo_service_impl.h"

using grpc::Server;
using grpc::ServerBuilder;

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    limestone::grpc::service::echo_service_impl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    
    LOG(INFO) << "Echo server listening on " << server_address;
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;  // Log to stderr instead of file
    
    LOG(INFO) << "Starting limestone gRPC echo server";
    RunServer();
    return 0;
}
