#pragma once

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "limestone/grpc/backend/grpc_service_backend.h"
#include "limestone/grpc/service/ping_service.h"
#include <limestone/logging.h>
#include "logging_helper.h"
namespace limestone::grpc::testing {

class grpc_test_helper {
public:
    /**
     * @brief Returns the server address string (e.g. "127.0.0.1:50000").
     * @return server address
     */
    std::string const& server_address() const { return server_address_; }
public:
    /**
     * @brief Sets the backend factory function for the gRPC server.
     * @param f Factory function returning a unique_ptr to grpc_service_backend
     */
    void set_backend_factory(std::function<std::unique_ptr<limestone::grpc::backend::grpc_service_backend>()> f) {
        backend_factory_ = std::move(f);
    }


    /**
     * @brief Adds a service factory function for the gRPC server.
     * @param f Factory function returning a unique_ptr to grpc::Service
     */
    void add_service_factory(std::function<std::unique_ptr<::grpc::Service>(limestone::grpc::backend::grpc_service_backend&)> f) {
        service_factories_.push_back(std::move(f));
    }

    /**
     * @brief Starts the gRPC server using the configured factories.
     * @note backend_factory and at least one service_factory must be set before calling this.
     */
    void start_server() {
        ASSERT_TRUE(backend_factory_) << "backend_factory_ is not set. Call set_backend_factory() before start_server().";
        ASSERT_FALSE(service_factories_.empty()) << "At least one service_factory must be set before start_server().";
        backend_ = backend_factory_();
        ::grpc::ServerBuilder builder;

        int bound_port = 0; // Variable to store the actual bound port
        builder.AddListeningPort(server_address_, ::grpc::InsecureServerCredentials(), &bound_port);

        ping_service_ = std::make_unique<limestone::grpc::service::ping_service>();
        builder.RegisterService(ping_service_.get());
        for (auto& factory : service_factories_) {
            auto svc = factory(*backend_);
            builder.RegisterService(svc.get());
            services_.push_back(std::move(svc));
        }
        server_ = builder.BuildAndStart();
        ASSERT_TRUE(server_ != nullptr);

        // Log the server address and actual bound port
        server_address_ = "127.0.0.1:" + std::to_string(bound_port);
        LOG_LP(INFO) << "gRPC server started at: " << server_address_;
        wait_for_server_ready();
    }

    /**
     * @brief Initializes the server address (finds an available port).
     */
    void setup() {
        server_address_ = "127.0.0.1:0";
    }

    /**
     * @brief Shuts down the gRPC server and cleans up resources.
     */
    void tear_down() {
        LOG_LP(INFO) << "teaar down gRPC server start";
        if (server_) {
            server_->Shutdown();
            server_.reset();
        }
        services_.clear();
        backend_.reset();
        LOG_LP(INFO) << "tear down gRPC server done";
    }

    /**
     * @brief Waits until the gRPC server is ready to accept requests.
     * @throws std::runtime_error if the server does not become ready in time.
     */
    void wait_for_server_ready() {
        LOG_LP(INFO) << "Waiting for gRPC server to become ready...";
        constexpr int max_attempts = 50;
        constexpr int wait_millis = 10;
        for (int attempt = 0; attempt < max_attempts; ++attempt) {
            if (is_server_ready()) {
                LOG_LP(INFO) << "gRPC server is ready.";
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_millis));
        }
        throw std::runtime_error("gRPC server did not become ready in time");
    }

    /**
     * @brief Checks if the gRPC server is ready by pinging it.
     * @return true if the server responds to ping, false otherwise
     */
    virtual bool is_server_ready() {
        auto channel = ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials());
        limestone::grpc::proto::PingService::Stub stub(channel);
        ::grpc::ClientContext context;
        limestone::grpc::proto::PingRequest req;
        limestone::grpc::proto::PingResponse resp;
        auto status = stub.Ping(&context, req, &resp);
        return status.ok();
    }


    /**
     * @brief Checks if the specified port is available for binding.
     * @param port Port number to check
     * @return true if the port is available, false otherwise
     */
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

    /**
     * @brief Create a gRPC channel to the test server
     */
    std::shared_ptr<::grpc::Channel> create_channel() const {
        auto channel = ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials());
        LOG_LP(INFO) << "Created gRPC channel to " << server_address_;
        return channel;
    }

private:
    std::unique_ptr<::grpc::Server> server_;
    std::string server_address_;
    std::unique_ptr<limestone::grpc::service::ping_service> ping_service_;
    std::unique_ptr<limestone::grpc::backend::grpc_service_backend> backend_;
    std::vector<std::unique_ptr<::grpc::Service>> services_;
    std::function<std::unique_ptr<limestone::grpc::backend::grpc_service_backend>()> backend_factory_;
    std::vector<std::function<std::unique_ptr<::grpc::Service>(limestone::grpc::backend::grpc_service_backend&)>> service_factories_;
};

} // namespace limestone::grpc::testing
