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

#include <cstddef>
#include <cstdint>
#include <memory>

#include <rdma/rdma_comm/rdma_comm_constants.h>
#include <rdma/rdma_factory.h>
#include <rdma/rdma_receive_event.h>
#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_send_stream_base.h>
#include <rdma/rdma_sender_base.h>

namespace limestone::testing {

namespace {

/// @brief Largest min_capacity acquire_frame_buffer() accepts: one slot's payload.
constexpr std::size_t slot_payload = limestone::replication::rdma_slot_payload_bytes;

} // namespace

/**
 * @brief Boundary tests for rdma_comm_send_stream::acquire_frame_buffer()'s min_capacity.
 *
 * Builds a real send stream on the vendor mock. The receiver is initialized first
 * because libgnmock requires it before the sender can Initialize().
 */
class rdma_comm_send_stream_test : public ::testing::Test {
protected:
    void SetUp() override {
        constexpr std::uint32_t slot_count = 4U;
        constexpr std::uint64_t dma_address = 0x1234U;

        receiver_ = limestone::replication::make_rdma_data_receiver(slot_count);
        auto receiver_result =
            receiver_->initialize([](limestone::replication::rdma_receive_event const&) {});
        ASSERT_TRUE(receiver_result.success)
            << "receiver init failed: " << receiver_result.error_message;

        sender_ = limestone::replication::make_rdma_data_sender(slot_count);
        auto sender_result = sender_->initialize(dma_address);
        ASSERT_TRUE(sender_result.success)
            << "sender init failed: " << sender_result.error_message;

        auto acquired = sender_->get_send_stream(0U);
        ASSERT_TRUE(acquired.status.success)
            << "get_send_stream failed: " << acquired.status.error_message;
        stream_ = std::move(acquired.stream);

        auto finalize_result = sender_->finalize_channel_setup();
        ASSERT_TRUE(finalize_result.success)
            << "finalize_channel_setup failed: " << finalize_result.error_message;
    }

    void TearDown() override {
        stream_.reset();
        if (sender_) {
            (void) sender_->shutdown();
            sender_.reset();
        }
        if (receiver_) {
            (void) receiver_->shutdown();
            receiver_.reset();
        }
    }

    std::unique_ptr<limestone::replication::rdma_send_stream_base> stream_{};

private:
    std::unique_ptr<limestone::replication::rdma_receiver_base> receiver_{};
    std::unique_ptr<limestone::replication::rdma_sender_base> sender_{};
};

TEST_F(rdma_comm_send_stream_test, acquire_rejects_zero_min_capacity) {
    EXPECT_EQ(stream_->acquire_frame_buffer(1U, 0U), nullptr);
}

TEST_F(rdma_comm_send_stream_test, acquire_accepts_min_capacity_one) {
    auto frame = stream_->acquire_frame_buffer(1U, 1U);
    ASSERT_NE(frame, nullptr);
    EXPECT_GE(frame->capacity(), 1U);
}

TEST_F(rdma_comm_send_stream_test, acquire_accepts_min_capacity_equal_to_max_payload) {
    auto frame = stream_->acquire_frame_buffer(100U, 100U);
    ASSERT_NE(frame, nullptr);
    EXPECT_GE(frame->capacity(), 100U);
}

TEST_F(rdma_comm_send_stream_test, acquire_rejects_min_capacity_above_max_payload) {
    EXPECT_EQ(stream_->acquire_frame_buffer(100U, 101U), nullptr);
}

TEST_F(rdma_comm_send_stream_test, acquire_accepts_min_capacity_at_slot_payload) {
    auto frame = stream_->acquire_frame_buffer(slot_payload, slot_payload);
    ASSERT_NE(frame, nullptr);
    EXPECT_GE(frame->capacity(), slot_payload);
}

TEST_F(rdma_comm_send_stream_test, acquire_rejects_min_capacity_above_slot_payload) {
    EXPECT_EQ(stream_->acquire_frame_buffer(slot_payload + 100U, slot_payload + 1U), nullptr);
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
