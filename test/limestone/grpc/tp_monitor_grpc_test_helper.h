/*
 * Copyright 2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/service_type.h>
#include <gtest/gtest.h>

namespace limestone::grpc::testing {

class tp_monitor_grpc_test_helper {
public:
    /**
     * @brief Destroys the helper and tears down the test server.
     */
    ~tp_monitor_grpc_test_helper() { tear_down(); }
    /**
     * @brief Registers a service factory to be hosted by the test server.
     * @param factory Factory function returning a grpc::Service instance.
     */
    void add_service_factory(std::function<std::unique_ptr<::grpc::Service>()> factory);

    /**
     * @brief Starts the test gRPC server on an ephemeral port.
     */
    void start_server();

    /**
     * @brief Shuts down the test gRPC server and clears services.
     */
    void tear_down();

    /**
     * @brief Creates a channel to the test server.
     * @return Shared channel instance.
     */
    std::shared_ptr<::grpc::Channel> create_channel() const;

private:
    /**
     * @brief Waits until the server becomes ready for connections.
     */
    void wait_for_server_ready();

    std::unique_ptr<::grpc::Server> server_{};
    std::string server_address_{"127.0.0.1:0"};
    std::vector<std::unique_ptr<::grpc::Service>> services_{};
    std::vector<std::function<std::unique_ptr<::grpc::Service>()>> service_factories_{};
};

} // namespace limestone::grpc::testing
