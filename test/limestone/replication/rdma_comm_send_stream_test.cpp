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

#include <rdma/rdma_comm_send_stream.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <rdma_comm/rdma_sender.h>

namespace limestone::replication {
namespace {

// Base for the test fakes below. The wrapper under test only ever uses acquire_frame_buffer(),
// submit_frame_buffer(), and flush(); any call to the legacy send_* / take_ack_body() API is
// unexpected and fails the test loudly so the cause is easy to spot. Derived fakes implement
// acquire_frame_buffer() and submit_frame_buffer().
class fake_send_stream : public rdma::communication::rdma_send_stream {
public:
    send_result send_bytes(
            std::vector<std::uint8_t> const&, std::size_t, std::size_t) noexcept override {
        ADD_FAILURE() << "unexpected send_bytes() call on test fake";
        return {};
    }

    send_result send_all_bytes(
            std::vector<std::uint8_t> const&, std::size_t, std::size_t) noexcept override {
        ADD_FAILURE() << "unexpected send_all_bytes() call on test fake";
        return {};
    }

    send_result send_with_writer(std::size_t, buffer_writer) noexcept override {
        ADD_FAILURE() << "unexpected send_with_writer() call on test fake";
        return {};
    }

    std::optional<rdma::communication::ack_body> take_ack_body() noexcept override {
        ADD_FAILURE() << "unexpected take_ack_body() call on test fake";
        return std::nullopt;
    }

    flush_result flush(std::chrono::milliseconds) noexcept override {
        return {};  // benign: flush is a legitimate pass-through the wrapper may call
    }
};

// Fake rdma_send_stream whose submit_frame_buffer always throws. acquire_frame_buffer
// returns a valid frame backed by an internal buffer (pool_ stays null, so the frame's
// destructor releases nothing). This drives rdma_comm_send_stream::submit_guarded down
// its catch paths without any real RDMA hardware.
class throwing_submit_stream : public fake_send_stream {
public:
    enum class throw_mode { std_exception, unknown };

    explicit throwing_submit_stream(throw_mode mode) noexcept : mode_(mode) {}

    frame_buffer acquire_frame_buffer(std::size_t max_payload) noexcept override {
        frame_buffer frame{};
        frame.payload = buffer_.data();
        frame.capacity = std::min(max_payload, buffer_.size());
        return frame;
    }

    send_result submit_frame_buffer(frame_buffer&, std::size_t) override {
        if (mode_ == throw_mode::std_exception) {
            throw std::runtime_error("simulated submit failure");
        }
        throw 42;  // non-std type to exercise the catch(...) path
    }

private:
    throw_mode mode_;
    std::array<std::uint8_t, 256U> buffer_{};
};

std::unique_ptr<rdma_comm_send_stream> make_stream(throwing_submit_stream::throw_mode mode) {
    return std::make_unique<rdma_comm_send_stream>(
            std::make_unique<throwing_submit_stream>(mode));
}

// Fake rdma_send_stream whose submit_frame_buffer succeeds, recording every submitted
// frame. acquire_frame_buffer can be capped to a small capacity to force the multi-frame
// (ring-wrap-style partial) path in send_all_bytes / send_with_writer.
class succeeding_submit_stream : public fake_send_stream {
public:
    // frame_capacity_cap == 0 means "no cap": capacity equals the requested max_payload.
    explicit succeeding_submit_stream(std::size_t frame_capacity_cap = 0U) noexcept
        : frame_capacity_cap_(frame_capacity_cap) {}

    frame_buffer acquire_frame_buffer(std::size_t max_payload) noexcept override {
        frame_buffer frame{};
        auto capacity = std::min(max_payload, buffer_.size());
        if (frame_capacity_cap_ != 0U) {
            capacity = std::min(capacity, frame_capacity_cap_);
        }
        frame.payload = buffer_.data();
        frame.capacity = capacity;
        return frame;
    }

    send_result submit_frame_buffer(frame_buffer&, std::size_t payload_size) override {
        ++submit_calls_;
        // Each frame writes from the start of buffer_, so concatenating the submitted
        // prefixes reconstructs the full payload in order.
        submitted_bytes_.insert(
                submitted_bytes_.end(), buffer_.data(), buffer_.data() + payload_size);
        return {true, "", payload_size};
    }

    std::size_t submit_calls_{};
    std::vector<std::uint8_t> submitted_bytes_{};

private:
    std::size_t frame_capacity_cap_;
    std::array<std::uint8_t, 256U> buffer_{};
};

struct stream_with_spy {
    succeeding_submit_stream* spy;
    std::unique_ptr<rdma_comm_send_stream> stream;
};

stream_with_spy make_succeeding_stream(std::size_t frame_capacity_cap = 0U) {
    auto fake = std::make_unique<succeeding_submit_stream>(frame_capacity_cap);
    auto* spy = fake.get();
    return {spy, std::make_unique<rdma_comm_send_stream>(std::move(fake))};
}

// Fake whose acquire_frame_buffer always fails (returns an invalid frame), simulating an
// unavailable/exhausted pool. submit_frame_buffer must never be reached.
class unavailable_frame_stream : public fake_send_stream {
public:
    frame_buffer acquire_frame_buffer(std::size_t) noexcept override {
        return {};  // invalid frame (payload == nullptr)
    }

    send_result submit_frame_buffer(frame_buffer&, std::size_t) override {
        ADD_FAILURE() << "submit_frame_buffer must not be called when acquire fails";
        return {};
    }
};

// Fake whose submit_frame_buffer succeeds but reports zero bytes written, exercising the
// defensive zero-progress guard in send_all_bytes.
class zero_bytes_submit_stream : public fake_send_stream {
public:
    frame_buffer acquire_frame_buffer(std::size_t max_payload) noexcept override {
        frame_buffer frame{};
        frame.payload = buffer_.data();
        frame.capacity = std::min(max_payload, buffer_.size());
        return frame;
    }

    send_result submit_frame_buffer(frame_buffer&, std::size_t) override {
        return {true, "", 0U};  // success but no progress
    }

private:
    std::array<std::uint8_t, 256U> buffer_{};
};

// Fake whose acquire_frame_buffer always grants a valid frame too small to satisfy any
// min_capacity > 1, so acquire_frame_min_capacity keeps retrying until it gives up. Drives
// the undersized-frame retry/give-up path. submit_frame_buffer must never be reached.
class undersized_frame_stream : public fake_send_stream {
public:
    frame_buffer acquire_frame_buffer(std::size_t max_payload) noexcept override {
        frame_buffer frame{};
        frame.payload = buffer_.data();
        frame.capacity = std::min<std::size_t>(1U, max_payload);  // never >= a min_capacity > 1
        return frame;
    }

    send_result submit_frame_buffer(frame_buffer&, std::size_t) override {
        ADD_FAILURE() << "submit_frame_buffer must not be called when min_capacity is never met";
        return {};
    }

private:
    std::array<std::uint8_t, 256U> buffer_{};
};

template <typename FakeStream>
std::unique_ptr<rdma_comm_send_stream> wrap(std::unique_ptr<FakeStream> fake) {
    return std::make_unique<rdma_comm_send_stream>(std::move(fake));
}

TEST(rdma_comm_send_stream_test, send_bytes_reports_submit_std_exception_as_failure) {
    auto stream = make_stream(throwing_submit_stream::throw_mode::std_exception);
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U, 0x04U};

    auto const result = stream->send_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_NE(result.error_message.find("threw"), std::string::npos);
    EXPECT_NE(result.error_message.find("simulated submit failure"), std::string::npos);
}

TEST(rdma_comm_send_stream_test, send_bytes_reports_submit_unknown_exception_as_failure) {
    auto stream = make_stream(throwing_submit_stream::throw_mode::unknown);
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U, 0x04U};

    auto const result = stream->send_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_NE(result.error_message.find("unknown exception"), std::string::npos);
}

TEST(rdma_comm_send_stream_test, send_all_bytes_reports_submit_exception_with_zero_progress) {
    auto stream = make_stream(throwing_submit_stream::throw_mode::std_exception);
    std::vector<std::uint8_t> payload{0x10U, 0x20U, 0x30U, 0x40U, 0x50U};

    auto const result = stream->send_all_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);  // nothing submitted before the throw
    EXPECT_NE(result.error_message.find("threw"), std::string::npos);
}

TEST(rdma_comm_send_stream_test, send_with_writer_reports_submit_exception_after_successful_fill) {
    auto stream = make_stream(throwing_submit_stream::throw_mode::std_exception);
    bool writer_invoked = false;

    auto const result = stream->send_with_writer(
            8U,
            [&writer_invoked](std::uint8_t* buffer, std::size_t capacity)
                    -> rdma_send_stream_base::buffer_fill_result {
                writer_invoked = true;
                std::fill_n(buffer, capacity, std::uint8_t{0xAB});
                return {true, ""};
            },
            0U);

    EXPECT_TRUE(writer_invoked);  // the buffer was filled before submission failed
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_NE(result.error_message.find("threw"), std::string::npos);
}

TEST(rdma_comm_send_stream_test, send_bytes_succeeds_and_reports_bytes_written) {
    auto fixture = make_succeeding_stream();
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U, 0x04U};

    auto const result = fixture.stream->send_bytes(payload, 0U, payload.size());

    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.error_message.empty());
    EXPECT_EQ(result.bytes_written, payload.size());
    EXPECT_EQ(fixture.spy->submit_calls_, 1U);
    EXPECT_EQ(fixture.spy->submitted_bytes_, payload);
}

TEST(rdma_comm_send_stream_test, send_bytes_honours_offset_and_length) {
    auto fixture = make_succeeding_stream();
    std::vector<std::uint8_t> payload{0x10U, 0x11U, 0x12U, 0x13U, 0x14U};

    auto const result = fixture.stream->send_bytes(payload, 1U, 3U);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 3U);
    EXPECT_EQ(fixture.spy->submitted_bytes_,
              (std::vector<std::uint8_t>{0x11U, 0x12U, 0x13U}));
}

TEST(rdma_comm_send_stream_test, send_with_writer_fills_and_submits_one_frame) {
    auto fixture = make_succeeding_stream();

    auto const result = fixture.stream->send_with_writer(
            5U,
            [](std::uint8_t* buffer, std::size_t capacity)
                    -> rdma_send_stream_base::buffer_fill_result {
                for (std::size_t i = 0; i < capacity; ++i) {
                    buffer[i] = static_cast<std::uint8_t>(0xA0U + i);
                }
                return {true, ""};
            },
            0U);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 5U);
    EXPECT_EQ(fixture.spy->submit_calls_, 1U);
    EXPECT_EQ(fixture.spy->submitted_bytes_,
              (std::vector<std::uint8_t>{0xA0U, 0xA1U, 0xA2U, 0xA3U, 0xA4U}));
}

TEST(rdma_comm_send_stream_test, send_all_bytes_splits_across_frames_when_capacity_is_capped) {
    // Cap each frame to 2 bytes so a 5-byte payload needs three frames; send_all_bytes
    // must loop until the whole payload is submitted, in order.
    auto fixture = make_succeeding_stream(/*frame_capacity_cap=*/2U);
    std::vector<std::uint8_t> payload{0x21U, 0x22U, 0x23U, 0x24U, 0x25U};

    auto const result = fixture.stream->send_all_bytes(payload, 0U, payload.size());

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, payload.size());
    EXPECT_EQ(fixture.spy->submit_calls_, 3U);  // 2 + 2 + 1
    EXPECT_EQ(fixture.spy->submitted_bytes_, payload);
}

TEST(rdma_comm_send_stream_test, send_bytes_rejects_offset_beyond_payload) {
    auto fixture = make_succeeding_stream();
    std::vector<std::uint8_t> payload{0x01U, 0x02U};

    auto const result = fixture.stream->send_bytes(payload, payload.size() + 1U, 1U);

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_NE(result.error_message.find("offset exceeds"), std::string::npos);
    EXPECT_EQ(fixture.spy->submit_calls_, 0U);
}

TEST(rdma_comm_send_stream_test, send_bytes_with_zero_length_is_success_noop) {
    auto fixture = make_succeeding_stream();
    std::vector<std::uint8_t> payload{0x01U, 0x02U};

    auto const result = fixture.stream->send_bytes(payload, 0U, 0U);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_EQ(fixture.spy->submit_calls_, 0U);  // nothing acquired or submitted
}

TEST(rdma_comm_send_stream_test, send_bytes_reports_acquire_failure) {
    auto stream = wrap(std::make_unique<unavailable_frame_stream>());
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U};

    auto const result = stream->send_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_FALSE(result.error_message.empty());
}

TEST(rdma_comm_send_stream_test, send_all_bytes_rejects_offset_beyond_payload) {
    auto fixture = make_succeeding_stream();
    std::vector<std::uint8_t> payload{0x01U, 0x02U};

    auto const result = fixture.stream->send_all_bytes(payload, payload.size() + 1U, 1U);

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_NE(result.error_message.find("offset exceeds"), std::string::npos);
    EXPECT_EQ(fixture.spy->submit_calls_, 0U);
}

TEST(rdma_comm_send_stream_test, send_all_bytes_reports_acquire_failure_with_zero_progress) {
    auto stream = wrap(std::make_unique<unavailable_frame_stream>());
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U};

    auto const result = stream->send_all_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_FALSE(result.error_message.empty());
}

TEST(rdma_comm_send_stream_test, send_all_bytes_reports_zero_byte_submit_as_failure) {
    auto stream = wrap(std::make_unique<zero_bytes_submit_stream>());
    std::vector<std::uint8_t> payload{0x01U, 0x02U, 0x03U};

    auto const result = stream->send_all_bytes(payload, 0U, payload.size());

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);  // submit reported zero progress
    EXPECT_NE(result.error_message.find("zero bytes"), std::string::npos);
}

TEST(rdma_comm_send_stream_test, send_with_writer_with_zero_remaining_is_success_noop) {
    auto fixture = make_succeeding_stream();
    bool writer_invoked = false;

    auto const result = fixture.stream->send_with_writer(
            0U,
            [&writer_invoked](std::uint8_t*, std::size_t)
                    -> rdma_send_stream_base::buffer_fill_result {
                writer_invoked = true;
                return {true, ""};
            },
            0U);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_FALSE(writer_invoked);  // callback must not run for an empty payload
    EXPECT_EQ(fixture.spy->submit_calls_, 0U);
}

TEST(rdma_comm_send_stream_test, send_with_writer_reports_acquire_failure) {
    auto stream = wrap(std::make_unique<unavailable_frame_stream>());

    auto const result = stream->send_with_writer(
            4U,
            [](std::uint8_t*, std::size_t) -> rdma_send_stream_base::buffer_fill_result {
                return {true, ""};
            },
            0U);

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_FALSE(result.error_message.empty());
}

TEST(rdma_comm_send_stream_test, send_with_writer_reports_writer_failure_without_submitting) {
    auto fixture = make_succeeding_stream();

    auto const result = fixture.stream->send_with_writer(
            4U,
            [](std::uint8_t*, std::size_t) -> rdma_send_stream_base::buffer_fill_result {
                return {false, "writer rejected the buffer"};
            },
            0U);

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_EQ(result.error_message, "writer rejected the buffer");
    EXPECT_EQ(fixture.spy->submit_calls_, 0U);  // failed fill must not submit
}

TEST(rdma_comm_send_stream_test, send_with_writer_fails_when_capacity_never_reaches_min) {
    // Deterministic and fast: give-up happens after a fixed, compile-time retry bound
    // (max_undersized_retries) and the in-loop wait is std::this_thread::yield() (a
    // non-blocking scheduler hint, not a sleep), so there is no wall-clock dependency.
    auto stream = wrap(std::make_unique<undersized_frame_stream>());
    bool writer_invoked = false;

    auto const result = stream->send_with_writer(
            20U,
            [&writer_invoked](std::uint8_t*, std::size_t)
                    -> rdma_send_stream_base::buffer_fill_result {
                writer_invoked = true;
                return {true, ""};
            },
            10U);  // min_capacity = 10, but the fake never grants a frame that large

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.bytes_written, 0U);
    EXPECT_FALSE(writer_invoked);  // give-up happens before the writer is ever invoked
    EXPECT_FALSE(result.error_message.empty());
}

TEST(rdma_comm_send_stream_test, send_with_writer_clamps_min_capacity_to_remaining_size) {
    // min_capacity larger than the whole payload must be clamped to remaining_size: the
    // writer is still invoked (with capacity == remaining_size) and the send succeeds.
    auto fixture = make_succeeding_stream();
    std::size_t writer_capacity = 0U;

    auto const result = fixture.stream->send_with_writer(
            4U,
            [&writer_capacity](std::uint8_t* buffer, std::size_t capacity)
                    -> rdma_send_stream_base::buffer_fill_result {
                writer_capacity = capacity;
                std::fill_n(buffer, capacity, std::uint8_t{0x7F});
                return {true, ""};
            },
            100U);  // min_capacity far exceeds remaining_size -> clamped to 4

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.bytes_written, 4U);
    EXPECT_EQ(writer_capacity, 4U);  // invoked with remaining_size, never min_capacity
    EXPECT_EQ(fixture.spy->submit_calls_, 1U);
}

}  // namespace
}  // namespace limestone::replication

#endif  // LIMESTONE_ENABLE_RDMA
