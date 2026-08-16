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

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include <rdma/rdma_handshake_payload.h>

namespace limestone::testing {

using limestone::replication::decode_response_payload;
using limestone::replication::decode_start_payload;
using limestone::replication::encode;
using limestone::replication::rdma_handshake_response_payload;
using limestone::replication::rdma_handshake_start_payload;

namespace {

rdma_handshake_start_payload make_start_payload() {
    rdma_handshake_start_payload payload{};
    payload.protocol_version = 2U;
    payload.configuration_id = "configuration-abc";
    payload.epoch_number = 12345U;
    payload.slot_count = 128U;
    payload.master_dma_address = 0x1122334455667788ULL;
    payload.channel_count = 3U;
    payload.control_channel_id = 5U;
    return payload;
}

} // namespace

TEST(rdma_handshake_payload_test, start_payload_round_trips) {
    auto const payload = make_start_payload();
    auto const decoded = decode_start_payload(encode(payload));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, payload);
}

TEST(rdma_handshake_payload_test, start_payload_round_trips_boundary_values) {
    rdma_handshake_start_payload payload{};
    payload.protocol_version = std::numeric_limits<std::uint64_t>::max();
    payload.configuration_id = "";
    payload.epoch_number = std::numeric_limits<std::uint64_t>::max();
    payload.slot_count = std::numeric_limits<std::uint32_t>::max();
    payload.master_dma_address = std::numeric_limits<std::uint64_t>::max();
    payload.channel_count = std::numeric_limits<std::uint16_t>::max();
    payload.control_channel_id = std::numeric_limits<std::uint16_t>::max();
    auto const decoded = decode_start_payload(encode(payload));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, payload);
}

TEST(rdma_handshake_payload_test, start_payload_encodes_expected_layout) {
    rdma_handshake_start_payload payload{};
    payload.protocol_version = 2U;
    payload.configuration_id = "cfg";
    payload.epoch_number = 7U;
    payload.slot_count = 16U;
    payload.master_dma_address = 0x1122334455667788ULL;
    payload.channel_count = 3U;
    payload.control_channel_id = 5U;
    std::vector<std::uint8_t> const expected{
        0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x02U,  // protocol_version
        0x00U, 0x00U, 0x00U, 0x03U, 'c', 'f', 'g',               // configuration_id
        0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x00U, 0x07U,  // epoch_number
        0x00U, 0x00U, 0x00U, 0x10U,                              // slot_count
        0x11U, 0x22U, 0x33U, 0x44U, 0x55U, 0x66U, 0x77U, 0x88U,  // master_dma_address
        0x00U, 0x03U,                                            // channel_count
        0x00U, 0x05U,                                            // control_channel_id
    };
    EXPECT_EQ(encode(payload), expected);
}

TEST(rdma_handshake_payload_test, start_payload_decode_rejects_truncated_bytes) {
    auto const bytes = encode(make_start_payload());
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        std::vector<std::uint8_t> const truncated(bytes.begin(), bytes.begin() + size);
        EXPECT_FALSE(decode_start_payload(truncated).has_value()) << "prefix length " << size;
    }
}

TEST(rdma_handshake_payload_test, start_payload_decode_rejects_trailing_bytes) {
    auto bytes = encode(make_start_payload());
    bytes.push_back(0U);
    EXPECT_FALSE(decode_start_payload(bytes).has_value());
}

TEST(rdma_handshake_payload_test, start_payload_decode_rejects_oversized_length_prefix) {
    auto bytes = encode(make_start_payload());
    // The configuration_id length prefix sits right after the 8-byte protocol_version.
    for (std::size_t offset = 8; offset < 12; ++offset) {
        bytes.at(offset) = 0xFFU;
    }
    EXPECT_FALSE(decode_start_payload(bytes).has_value());
}

TEST(rdma_handshake_payload_test, response_payload_round_trips_accepted) {
    rdma_handshake_response_payload payload{};
    payload.accepted = true;
    payload.replica_dma_address = 0x8877665544332211ULL;
    auto const decoded = decode_response_payload(encode(payload));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, payload);
}

TEST(rdma_handshake_payload_test, response_payload_round_trips_rejected) {
    rdma_handshake_response_payload payload{};
    payload.accepted = false;
    payload.error_message = "configuration mismatch";
    payload.replica_dma_address = 0U;
    auto const decoded = decode_response_payload(encode(payload));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, payload);
}

TEST(rdma_handshake_payload_test, response_payload_encodes_expected_layout) {
    rdma_handshake_response_payload payload{};
    payload.accepted = false;
    payload.error_message = "no";
    payload.replica_dma_address = 0x0102030405060708ULL;
    std::vector<std::uint8_t> const expected{
        0x00U,                                                   // accepted
        0x00U, 0x00U, 0x00U, 0x02U, 'n', 'o',                    // error_message
        0x01U, 0x02U, 0x03U, 0x04U, 0x05U, 0x06U, 0x07U, 0x08U,  // replica_dma_address
    };
    EXPECT_EQ(encode(payload), expected);
}

TEST(rdma_handshake_payload_test, response_payload_decode_rejects_oversized_length_prefix) {
    rdma_handshake_response_payload payload{};
    payload.accepted = true;
    payload.error_message = "x";
    auto bytes = encode(payload);
    // The error_message length prefix sits right after the 1-byte accepted flag.
    for (std::size_t offset = 1; offset < 5; ++offset) {
        bytes.at(offset) = 0xFFU;
    }
    EXPECT_FALSE(decode_response_payload(bytes).has_value());
}

TEST(rdma_handshake_payload_test, response_payload_decode_rejects_truncated_bytes) {
    rdma_handshake_response_payload payload{};
    payload.accepted = false;
    payload.error_message = "reason";
    payload.replica_dma_address = 42U;
    auto const bytes = encode(payload);
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        std::vector<std::uint8_t> const truncated(bytes.begin(), bytes.begin() + size);
        EXPECT_FALSE(decode_response_payload(truncated).has_value()) << "prefix length " << size;
    }
}

TEST(rdma_handshake_payload_test, response_payload_decode_rejects_trailing_bytes) {
    rdma_handshake_response_payload payload{};
    payload.accepted = true;
    auto bytes = encode(payload);
    bytes.push_back(0U);
    EXPECT_FALSE(decode_response_payload(bytes).has_value());
}

TEST(rdma_handshake_payload_test, response_payload_decode_rejects_invalid_accepted_flag) {
    rdma_handshake_response_payload payload{};
    payload.accepted = true;
    auto bytes = encode(payload);
    bytes.at(0) = 2U;
    EXPECT_FALSE(decode_response_payload(bytes).has_value());
}

} // namespace limestone::testing
