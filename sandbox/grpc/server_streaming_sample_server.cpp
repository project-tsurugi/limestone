// Command-line server for server-streaming sample services
#include <grpcpp/grpcpp.h>
#include "grpc/service/server_streaming_sample_service.h"
#include <iostream>

int main(int argc, char* argv[]) {
    std::string server_address = "0.0.0.0:50051";
    if (argc > 1) {
        server_address = argv[1];
    }
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024); // 64MB
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);    // 64MB
    builder.SetMaxMessageSize(64 * 1024 * 1024);        // 64MB
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    limestone::grpc::service::FileSizeServiceImpl file_size_service;
    limestone::grpc::service::RandomBytesServiceImpl random_bytes_service;
    builder.RegisterService(&file_size_service);
    builder.RegisterService(&random_bytes_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
    return 0;
}
