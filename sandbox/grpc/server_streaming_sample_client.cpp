// Command-line client for server-streaming sample services
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <glog/logging.h>
#include <fstream>
#include <vector>
#include <string>
#include "server_streaming_sample.pb.h"
#include "server_streaming_sample.grpc.pb.h"

void print_usage(const char* prog_name) {
    std::cout << "Usage: " << prog_name << " <service> <server_address> [options]\n"
              << "  service: file_size | random_bytes\n"
              << "  server_address: host:port\n"
              << "  file_size options: <file_path>\n"
              << "  random_bytes options: <size>\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }
    std::string service = argv[1];
    std::string server_address = argv[2];
    grpc::ChannelArguments args;
    args.SetMaxSendMessageSize(64 * 1024 * 1024);    // 64MB
    args.SetMaxReceiveMessageSize(64 * 1024 * 1024); // 64MB
    auto channel = grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), args);

    if (service == "file_size") {
        using namespace std::chrono;
        LOG(INFO) << "[file_size] start";
        auto t0 = steady_clock::now();
        if (argc < 4) {
            std::cerr << "Missing file_path argument for file_size service\n";
            return 1;
        }
        std::string file_path = argv[3];
        std::ifstream file(file_path, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to open file: " << file_path << "\n";
            return 1;
        }
        limestone::grpc::FileSizeService::Stub stub(channel);
        grpc::ClientContext context;
        limestone::grpc::FileSizeResponse response;
        std::unique_ptr<grpc::ClientWriter<limestone::grpc::FileChunk>> writer(
            stub.GetFileSize(&context, &response));
        constexpr size_t buffer_size = 32 * 1024 * 1024; // 32MB
        std::vector<char> buffer(buffer_size);
        while (file.read(buffer.data(), buffer.size()) || file.gcount() > 0) {
            limestone::grpc::FileChunk chunk;
            chunk.set_data(std::string(buffer.data(), file.gcount()));
            if (!writer->Write(chunk)) {
                std::cerr << "Failed to write chunk to server\n";
                break;
            }
        }
        writer->WritesDone();
        grpc::Status status = writer->Finish();
        auto t1 = steady_clock::now();
        auto ms = duration_cast<milliseconds>(t1 - t0).count();
        if (status.ok()) {
            std::cout << "File size: " << response.size() << " bytes\n";
        } else {
            std::cerr << "RPC failed: " << status.error_message() << "\n";
        }
        LOG(INFO) << "[file_size] end: elapsed " << ms << " ms";
    } else if (service == "random_bytes") {
        using namespace std::chrono;
        LOG(INFO) << "[random_bytes] start";
        auto t0 = steady_clock::now();
        if (argc < 4) {
            std::cerr << "Missing size argument for random_bytes service\n";
            return 1;
        }
        int64_t size = std::stoll(argv[3]);
        limestone::grpc::RandomBytesService::Stub stub(channel);
        grpc::ClientContext context;
        limestone::grpc::RandomBytesRequest request;
        request.set_size(size);
        std::unique_ptr<grpc::ClientReader<limestone::grpc::RandomBytesChunk>> reader(
            stub.GenerateRandomBytes(&context, request));
        int64_t received = 0;
        limestone::grpc::RandomBytesChunk chunk;
        while (reader->Read(&chunk)) {
            received += chunk.data().size();
            // For demonstration, do not print the data
        }
        grpc::Status status = reader->Finish();
        auto t1 = steady_clock::now();
        auto ms = duration_cast<milliseconds>(t1 - t0).count();
        if (status.ok()) {
            std::cout << "Received " << received << " bytes of random data\n";
        } else {
            std::cerr << "RPC failed: " << status.error_message() << "\n";
        }
        LOG(INFO) << "[random_bytes] end: elapsed " << ms << " ms";
    } else {
        std::cerr << "Unknown service: " << service << "\n";
        print_usage(argv[0]);
        return 1;
    }
    return 0;
}
