#ifndef LIMESTONE_GRPC_SERVER_TEST_BASE_H_
#define LIMESTONE_GRPC_SERVER_TEST_BASE_H_


#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "limestone/grpc/service/ping_service.h"

namespace limestone::grpc::testing {


class grpc_server_test_base : public ::testing::Test {
protected:
    std::unique_ptr<::grpc::Server> server_;
    std::string server_address_;
    std::unique_ptr<limestone::grpc::service::ping_service> ping_service_;

    void SetUp() override {
        server_address_ = find_available_address();
        server_ = build_and_start_server();
        ASSERT_TRUE(server_ != nullptr);
        wait_for_server_ready();
    }

    void TearDown() override {
        if (server_) server_->Shutdown();
    }

    // Derived class can override to register additional services
    virtual void register_additional_services(::grpc::ServerBuilder& builder) {}

    // Build and start the server
    std::unique_ptr<::grpc::Server> build_and_start_server() {
        ::grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address_, ::grpc::InsecureServerCredentials());
        // Always register ping_service
        ping_service_ = std::make_unique<limestone::grpc::service::ping_service>();
        builder.RegisterService(ping_service_.get());
        // Register additional services from derived class
        register_additional_services(builder);
        return builder.BuildAndStart();
    }

    // Wait until the server is ready (using ping_service)
    void wait_for_server_ready() {
        constexpr int max_attempts = 50;
        constexpr int wait_millis = 10;
        for (int attempt = 0; attempt < max_attempts; ++attempt) {
            if (is_server_ready()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_millis));
        }
        throw std::runtime_error("gRPC server did not become ready in time");
    }

    // Use ping_service to check if server is ready
    virtual bool is_server_ready() {
        auto channel = ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials());
        limestone::grpc::proto::PingService::Stub stub(channel);
        ::grpc::ClientContext context;
        limestone::grpc::proto::PingRequest req;
        limestone::grpc::proto::PingResponse resp;
        auto status = stub.Ping(&context, req, &resp);
        return status.ok();
    }

    // Find and return an available port number in the range 50000-50200
    std::string find_available_address() {
        for (int port = 50000; port <= 50200; ++port) {
            std::string address = "127.0.0.1:" + std::to_string(port);
            if (is_port_available(port)) {
                return address;
            }
        }
        throw std::runtime_error("No available port found in range 50000-50200");
    }

    // Check if the port is available
    bool is_port_available(int port) {
        int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;
        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        int result = ::bind(sock, (struct sockaddr*)&addr, sizeof(addr));
        ::close(sock);
        return result == 0;
    }
};

} // namespace limestone::grpc::testing

#endif // LIMESTONE_GRPC_SERVER_TEST_BASE_H_
