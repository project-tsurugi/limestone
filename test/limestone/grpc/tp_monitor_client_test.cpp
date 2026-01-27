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
#include <cstdint>
#include <future>
#include <memory>

#include <grpc/client/tp_monitor_client.h>
#include <limestone/grpc/service/tp_monitor_service_impl.h>
#include <limestone/grpc/tp_monitor_grpc_test_helper.h>
#include <test_root.h>

namespace limestone::testing {

class tp_monitor_client_test : public ::testing::Test {
protected:
    void SetUp() override {
        helper_.add_service_factory([]() {
            return std::make_unique<limestone::grpc::service::tp_monitor_service_impl>();
        });
        helper_.start_server();
        client_ = std::make_unique<limestone::grpc::client::tp_monitor_client>(
                helper_.create_channel());
    }

    void TearDown() override {
        helper_.tear_down();
        client_.reset();
    }

    limestone::grpc::testing::tp_monitor_grpc_test_helper helper_{};
    std::unique_ptr<limestone::grpc::client::tp_monitor_client> client_{};
};

TEST_F(tp_monitor_client_test, create_join_barrier_destroy_flow) { // NOLINT
    auto create_result = client_->create("tx-1", 1U);
    ASSERT_TRUE(create_result.ok);
    ASSERT_TRUE(create_result.tpm_id != 0U);

    auto join_result = client_->join(create_result.tpm_id, "tx-2", 2U);
    ASSERT_TRUE(join_result.ok);

    auto first_notify_future = std::async(std::launch::async, [this, &create_result]() {
        return client_->barrier_notify(create_result.tpm_id, 1U);
    });
    auto notify_result = client_->barrier_notify(create_result.tpm_id, 2U);
    EXPECT_TRUE(notify_result.ok);
    EXPECT_TRUE(first_notify_future.get().ok);

    auto destroy_result = client_->destroy(create_result.tpm_id);
    EXPECT_TRUE(destroy_result.ok);
}

TEST_F(tp_monitor_client_test, create_and_join_flow) { // NOLINT
    auto create_result = client_->create_and_join("tx-1", 1U, "tx-2", 2U);
    ASSERT_TRUE(create_result.ok);
    ASSERT_TRUE(create_result.tpm_id != 0U);

    auto first_notify_future = std::async(std::launch::async, [this, &create_result]() {
        return client_->barrier_notify(create_result.tpm_id, 1U);
    });
    auto notify_result = client_->barrier_notify(create_result.tpm_id, 2U);
    EXPECT_TRUE(notify_result.ok);
    EXPECT_TRUE(first_notify_future.get().ok);

    auto destroy_result = client_->destroy(create_result.tpm_id);
    EXPECT_TRUE(destroy_result.ok);
}

} // namespace limestone::testing
