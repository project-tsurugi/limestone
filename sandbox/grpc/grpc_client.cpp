#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include "limestone/grpc/client/echo_client.h"

int main(int argc, char** argv) {
    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;  // Log to stderr instead of file
    
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

    LOG(INFO) << "Starting limestone gRPC echo client";
    LOG(INFO) << "Connecting to: " << target_str;
    
    try {
        // Create echo client
        limestone::grpc::client::echo_client client(target_str);
        
        // Send echo request
        std::string response;
        ::grpc::Status status = client.echo(message, response);
        
        if (status.ok()) {
            std::cout << "Server replied: " << response << std::endl;
            LOG(INFO) << "Echo successful: " << response;
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
            LOG(ERROR) << "RPC failed: " << status.error_code() 
                       << ": " << status.error_message();
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        LOG(ERROR) << "Exception occurred: " << e.what();
        return 1;
    }

    return 0;
}
