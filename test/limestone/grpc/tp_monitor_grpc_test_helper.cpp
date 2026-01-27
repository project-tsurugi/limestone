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
#include <limestone/grpc/tp_monitor_grpc_test_helper.h>

namespace limestone::grpc::testing {

void tp_monitor_grpc_test_helper::add_service_factory(
        std::function<std::unique_ptr<::grpc::Service>()> factory) {
    service_factories_.push_back(std::move(factory));
}

void tp_monitor_grpc_test_helper::start_server() {
    ASSERT_TRUE(! service_factories_.empty());
    ::grpc::ServerBuilder builder;
    int bound_port = 0;
    builder.AddListeningPort(server_address_,
                             ::grpc::InsecureServerCredentials(),
                             &bound_port);

    for (auto& factory : service_factories_) {
        auto service = factory();
        builder.RegisterService(service.get());
        services_.push_back(std::move(service));
    }

    server_ = builder.BuildAndStart();
    ASSERT_TRUE(server_ != nullptr);
    server_address_ = "127.0.0.1:" + std::to_string(bound_port);
    wait_for_server_ready();
}

void tp_monitor_grpc_test_helper::tear_down() {
    if (server_) {
        server_->Shutdown();
        server_.reset();
    }
    services_.clear();
}

std::shared_ptr<::grpc::Channel> tp_monitor_grpc_test_helper::create_channel() const {
    return ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials());
}

void tp_monitor_grpc_test_helper::wait_for_server_ready() {
    auto channel = create_channel();
    constexpr int max_attempts = 50;
    constexpr auto wait_step = std::chrono::milliseconds(10);
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        auto deadline = std::chrono::system_clock::now() + wait_step;
        if (channel->WaitForConnected(deadline)) {
            return;
        }
        std::this_thread::sleep_for(wait_step);
    }
    FAIL() << "gRPC server did not become ready in time";
}

} // namespace limestone::grpc::testing
