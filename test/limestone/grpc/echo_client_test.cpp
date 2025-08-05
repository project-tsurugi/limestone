/*
 * Copyright 2022-2025 Project Tsurugi.
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



#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "limestone/grpc/client/echo_client.h"
#include "limestone/grpc/service/echo_service_impl.h"
#include "echo_service.grpc.pb.h"
#include "grpc_server_test_base.h"



namespace limestone::grpc::client::testing {


class echo_client_test : public limestone::grpc::testing::grpc_server_test_base {
protected:
    std::unique_ptr<limestone::grpc::service::echo_service_impl> service_;
    std::unique_ptr<echo_client> client_;

    void register_additional_services(::grpc::ServerBuilder& builder) override {
        service_ = std::make_unique<limestone::grpc::service::echo_service_impl>();
        builder.RegisterService(service_.get());
    }

    void SetUp() override {
        grpc_server_test_base::SetUp();
        client_ = std::make_unique<echo_client>(server_address_);
    }
};

TEST_F(echo_client_test, echo_returns_input_message) {
    std::string test_message = "test message";
    std::string response;
    auto status = client_->echo(test_message, response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response, test_message);
}

} // namespace limestone::grpc::client::testing
