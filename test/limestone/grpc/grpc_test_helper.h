#pragma once

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <functional>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "limestone/grpc/service/ping_service.h"
#include "limestone/grpc/backend/grpc_service_backend.h"

namespace limestone::grpc::testing {

class grpc_test_helper : public ::testing::Test {
protected:
    std::unique_ptr<::grpc::Server> server_;
    std::string server_address_;
    std::unique_ptr<limestone::grpc::service::ping_service> ping_service_;
    std::unique_ptr<limestone::grpc::backend::grpc_service_backend> backend_;
    std::unique_ptr<::grpc::Service> service_;
    std::function<std::unique_ptr<limestone::grpc::backend::grpc_service_backend>()> backend_factory_;
    std::function<std::unique_ptr<::grpc::Service>(limestone::grpc::backend::grpc_service_backend&)> service_factory_;

    void set_backend_factory(std::function<std::unique_ptr<limestone::grpc::backend::grpc_service_backend>()> f) {
        backend_factory_ = std::move(f);
    }
    void set_service_factory(std::function<std::unique_ptr<::grpc::Service>(limestone::grpc::backend::grpc_service_backend&)> f) {
        service_factory_ = std::move(f);
    }

    void start_server() {
        ASSERT_TRUE(backend_factory_) << "backend_factory_ is not set. Call set_backend_factory() before start_server().";
        ASSERT_TRUE(service_factory_) << "service_factory_ is not set. Call set_service_factory() before start_server().";
        backend_ = backend_factory_();
        service_ = service_factory_(*backend_);
        ::grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address_, ::grpc::InsecureServerCredentials());
        ping_service_ = std::make_unique<limestone::grpc::service::ping_service>();
        builder.RegisterService(ping_service_.get());
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        ASSERT_TRUE(server_ != nullptr);
        wait_for_server_ready();
    }

    void SetUp() override {
        server_address_ = find_available_address();
    }

    void TearDown() override {
        if (server_) {
            server_->Shutdown();
            server_.reset();
        }
        service_.reset();
        backend_.reset();
    }

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

    virtual bool is_server_ready() {
        auto channel = ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials());
        limestone::grpc::proto::PingService::Stub stub(channel);
        ::grpc::ClientContext context;
        limestone::grpc::proto::PingRequest req;
        limestone::grpc::proto::PingResponse resp;
        auto status = stub.Ping(&context, req, &resp);
        return status.ok();
    }

    std::string find_available_address() {
        for (int port = 50000; port <= 50200; ++port) {
            std::string address = "127.0.0.1:" + std::to_string(port);
            if (is_port_available(port)) {
                return address;
            }
        }
        throw std::runtime_error("No available port found in range 50000-50200");
    }

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
