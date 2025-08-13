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
#include "limestone/grpc/service/echo_service_impl.h"
#include "echo_service.grpc.pb.h"

namespace limestone::grpc::service::testing {

class echo_service_impl_test : public ::testing::Test {
protected:
    echo_service_impl service_;
};

TEST_F(echo_service_impl_test, echo_returns_input_message) {
    EchoRequest request;
    EchoResponse response;
    ::grpc::ServerContext context;

    std::string test_message = "Hello, gRPC!";
    request.set_message(test_message);

    auto status = service_.Echo(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.message(), test_message);
}

} // namespace limestone::grpc::service::testing
