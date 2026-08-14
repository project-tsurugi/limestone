/*
 * Copyright 2022-2026 Project Tsurugi.
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
#ifdef LIMESTONE_ENABLE_RDMA

#include <gtest/gtest.h>
#include <limestone/datastore_impl.h>

#include <cstdint>
#include <memory>

#include <rdma/rdma_factory.h>
#include <rdma/rdma_receiver_base.h>

namespace limestone::api {

// Helper that initializes the RDMA receiver before each RDMA sender test and
// shuts it down afterwards.  libgnmock requires the receiver to be initialized
// before the sender can Initialize(); without this the vendor mock returns
// "[Sender] semaphores not ready; receiver must prepare."
class rdma_sender_test : public ::testing::Test {
protected:
    void SetUp() override {
        receiver_ = make_rdma_data_receiver(4U);
        auto result = receiver_->initialize([](replication::rdma_receive_event const&) {});
        ASSERT_TRUE(result.success) << "receiver init failed: " << result.error_message;
    }

    void TearDown() override {
        if (receiver_) {
            receiver_->shutdown();
            receiver_.reset();
        }
    }

private:
    std::unique_ptr<replication::rdma_receiver_base> receiver_{};
};

TEST_F(rdma_sender_test, initialize_rdma_sender_success_sets_sender) {
    datastore_impl datastore;

    constexpr uint32_t test_slot_count = 4U;
    constexpr uint64_t test_dma_address = 0x1234U;

    EXPECT_TRUE(datastore.initialize_rdma_sender(test_slot_count, test_dma_address));
    EXPECT_NE(datastore.get_rdma_sender(), nullptr);
}

TEST_F(rdma_sender_test, shutdown_rdma_sender_after_initialize_clears_sender) {
    datastore_impl datastore;

    constexpr uint32_t test_slot_count = 4U;
    constexpr uint64_t test_dma_address = 0x1234U;

    ASSERT_TRUE(datastore.initialize_rdma_sender(test_slot_count, test_dma_address));
    ASSERT_NE(datastore.get_rdma_sender(), nullptr);

    EXPECT_TRUE(datastore.shutdown_rdma_sender());
    EXPECT_EQ(datastore.get_rdma_sender(), nullptr);
}

} // namespace limestone::api

#endif // LIMESTONE_ENABLE_RDMA
