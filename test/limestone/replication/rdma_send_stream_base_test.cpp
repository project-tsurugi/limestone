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
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include <rdma/rdma_send_stream_base.h>
#include <limestone/replication/test_rdma_frame_buffer.h>

namespace limestone::testing {

namespace {

/**
 * @brief Stub stream driving the non-virtual send_all_bytes() under test.
 *
 * Replays a scripted sequence of acquire grants and records every submitted
 * frame, so tests can assert on split positions, content, and failure paths.
 */
class scripted_rdma_send_stream : public limestone::replication::rdma_send_stream_base {
public:
    /// @brief Planned acquire results: nullopt fails the call, a value is the granted capacity.
    std::deque<std::optional<std::size_t>> grants_{};
    /// @brief 0-based index of the submit call to fail; nullopt never fails.
    std::optional<std::size_t> fail_submit_at_{};

    /// @brief (max_payload, min_capacity) of each acquire_frame_buffer() call.
    std::vector<std::pair<std::size_t, std::size_t>> acquire_args_{};
    /// @brief Payload of each successfully submitted frame, in submission order.
    std::vector<std::vector<std::uint8_t>> submitted_{};

    [[nodiscard]] std::unique_ptr<limestone::replication::rdma_frame_buffer_base>
    acquire_frame_buffer(std::size_t max_payload, std::size_t min_capacity) noexcept override {
        acquire_args_.emplace_back(max_payload, min_capacity);
        if (grants_.empty()) {
            ADD_FAILURE() << "unexpected acquire_frame_buffer call";
            return nullptr;
        }
        auto grant = grants_.front();
        grants_.pop_front();
        if (! grant.has_value()) {
            return nullptr;
        }
        return std::make_unique<test_rdma_frame_buffer>(grant.value());
    }

    [[nodiscard]] send_result submit_frame_buffer(
            limestone::replication::rdma_frame_buffer_base& frame,
            std::size_t payload_size) override {
        if (fail_submit_at_.has_value() && submitted_.size() == fail_submit_at_.value()) {
            return {false, "injected submit failure", 0};
        }
        auto& test_frame = dynamic_cast<test_rdma_frame_buffer&>(frame);
        submitted_.emplace_back(test_frame.take_written(payload_size));
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds /*timeout*/) noexcept override {
        return {true, ""};
    }
};

[[nodiscard]] std::vector<std::uint8_t> to_bytes(std::string_view text) {
    return {text.begin(), text.end()};
}

} // namespace

TEST(rdma_send_stream_base_test, empty_payload_succeeds_without_acquiring) {
    scripted_rdma_send_stream stream{};

    auto result = stream.send_all_bytes("");

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_TRUE(stream.acquire_args_.empty());
    EXPECT_TRUE(stream.submitted_.empty());
}

TEST(rdma_send_stream_base_test, exact_capacity_sends_single_frame) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {5U};

    auto result = stream.send_all_bytes("hello");

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 5U);
    ASSERT_EQ(stream.submitted_.size(), 1U);
    EXPECT_EQ(stream.submitted_[0], to_bytes("hello"));
}

TEST(rdma_send_stream_base_test, oversized_grant_is_clamped_to_remaining) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {4096U};

    auto result = stream.send_all_bytes("abc");

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 3U);
    ASSERT_EQ(stream.submitted_.size(), 1U);
    EXPECT_EQ(stream.submitted_[0], to_bytes("abc"));
}

TEST(rdma_send_stream_base_test, undersized_grants_split_across_frames) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {4U, 4U, 4U};

    auto result = stream.send_all_bytes("0123456789");

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 10U);
    ASSERT_EQ(stream.submitted_.size(), 3U);
    EXPECT_EQ(stream.submitted_[0], to_bytes("0123"));
    EXPECT_EQ(stream.submitted_[1], to_bytes("4567"));
    EXPECT_EQ(stream.submitted_[2], to_bytes("89"));
    // Each acquire asks for the remaining bytes and accepts any progress (min 1).
    std::vector<std::pair<std::size_t, std::size_t>> const expected_args{
        {10U, 1U}, {6U, 1U}, {2U, 1U}};
    EXPECT_EQ(stream.acquire_args_, expected_args);
}

TEST(rdma_send_stream_base_test, acquire_failure_on_first_frame_reports_zero_progress) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {std::nullopt};

    auto result = stream.send_all_bytes("abc");

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_message, "failed to acquire an RDMA frame buffer");
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_TRUE(stream.submitted_.empty());
}

TEST(rdma_send_stream_base_test, acquire_failure_midway_reports_partial_progress) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {4U, std::nullopt};

    auto result = stream.send_all_bytes("0123456789");

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_message, "failed to acquire an RDMA frame buffer");
    EXPECT_EQ(result.bytes_written, 4U);
    ASSERT_EQ(stream.submitted_.size(), 1U);
    EXPECT_EQ(stream.submitted_[0], to_bytes("0123"));
}

TEST(rdma_send_stream_base_test, submit_failure_propagates_error_and_partial_progress) {
    scripted_rdma_send_stream stream{};
    stream.grants_ = {4U, 4U};
    stream.fail_submit_at_ = 1U;

    auto result = stream.send_all_bytes("01234567");

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.error_message, "injected submit failure");
    EXPECT_EQ(result.bytes_written, 4U);
    ASSERT_EQ(stream.submitted_.size(), 1U);
    EXPECT_EQ(stream.submitted_[0], to_bytes("0123"));
}

} // namespace limestone::testing
