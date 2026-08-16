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
#include <replication/replication_config_loader.h>

#include <limits>
#include <sstream>

#include <gtest/gtest.h>

namespace limestone::testing {

using limestone::replication::make_replication_config;
using limestone::replication::parse_service_id;
using limestone::replication::replication_config;
using limestone::replication::replication_config_input;
using limestone::replication::replication_mode;
using limestone::replication::to_string_view;

// --- value class ---

class replication_config_test : public ::testing::Test {};

TEST_F(replication_config_test, default_construction_disables_replication) {
    replication_config config{};
    EXPECT_EQ(config.mode(), replication_mode::none);
    EXPECT_TRUE(config.handshake_socket_path().empty());
    EXPECT_EQ(config.service_id(), 0U);
}

TEST_F(replication_config_test, equality_compares_all_members) {
    replication_config rdma1{replication_mode::rdma, "/tmp/hs.sock", 100};
    replication_config rdma2{replication_mode::rdma, "/tmp/hs.sock", 100};
    replication_config rdma3{replication_mode::rdma, "/tmp/hs.sock", 101};
    EXPECT_TRUE(rdma1 == rdma2);
    EXPECT_FALSE(rdma1 != rdma2);
    EXPECT_TRUE(rdma1 != rdma3);
    EXPECT_TRUE(replication_config{} == replication_config{});
    EXPECT_TRUE(rdma1 != replication_config{});
}

TEST_F(replication_config_test, to_string_view_covers_all_modes) {
    EXPECT_EQ(to_string_view(replication_mode::none), "none");
    EXPECT_EQ(to_string_view(replication_mode::tcp), "tcp");
    EXPECT_EQ(to_string_view(replication_mode::rdma), "rdma");
    EXPECT_EQ(to_string_view(static_cast<replication_mode>(255)), "unknown");
}

TEST_F(replication_config_test, ostream_operator_writes_mode_name) {
    std::ostringstream oss{};
    oss << replication_mode::rdma;
    EXPECT_EQ(oss.str(), "rdma");
}

TEST_F(replication_config_test, ostream_operator_writes_rdma_settings) {
    std::ostringstream oss{};
    oss << replication_config{replication_mode::rdma, "/tmp/hs.sock", 100};
    EXPECT_NE(oss.str().find("rdma"), std::string::npos);
    EXPECT_NE(oss.str().find("/tmp/hs.sock"), std::string::npos);
    EXPECT_NE(oss.str().find("100"), std::string::npos);
}

// --- semantic validation (make_replication_config) ---

TEST_F(replication_config_test, empty_input_disables_replication) {
    auto result = make_replication_config({});
    EXPECT_TRUE(result.ok);
    EXPECT_TRUE(result.error_message.empty());
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_test, endpoint_selects_tcp_mode) {
    replication_config_input input{};
    input.endpoint_defined = true;
    auto result = make_replication_config(input);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.config.mode(), replication_mode::tcp);
    EXPECT_TRUE(result.config.handshake_socket_path().empty());
    EXPECT_EQ(result.config.service_id(), 0U);
}

TEST_F(replication_config_test, socket_and_endpoint_together_are_ambiguous) {
    replication_config_input input{};
    input.handshake_socket = "/tmp/hs.sock";
    input.service_id = 100;
    input.endpoint_defined = true;
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("ambiguous"), std::string::npos);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_test, service_id_without_socket_is_rejected) {
    replication_config_input input{};
    input.service_id = 100;
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("incomplete"), std::string::npos);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_test, service_id_with_endpoint_but_without_socket_is_rejected) {
    replication_config_input input{};
    input.service_id = 100;
    input.endpoint_defined = true;
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

#ifdef LIMESTONE_ENABLE_RDMA

TEST_F(replication_config_test, socket_and_service_id_select_rdma_mode) {
    replication_config_input input{};
    input.handshake_socket = "/tmp/hs.sock";
    input.service_id = 100;
    auto result = make_replication_config(input);
    EXPECT_TRUE(result.ok);
    EXPECT_TRUE(result.error_message.empty());
    EXPECT_EQ(result.config.mode(), replication_mode::rdma);
    EXPECT_EQ(result.config.handshake_socket_path(), "/tmp/hs.sock");
    EXPECT_EQ(result.config.service_id(), 100U);
}

TEST_F(replication_config_test, rdma_mode_requires_service_id) {
    replication_config_input input{};
    input.handshake_socket = "/tmp/hs.sock";
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("service_id"), std::string::npos);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_test, empty_socket_path_is_rejected) {
    replication_config_input input{};
    input.handshake_socket = "";
    input.service_id = 100;
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("empty"), std::string::npos);
}

TEST_F(replication_config_test, reserved_service_ids_are_rejected) {
    for (std::uint64_t value : {std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max()}) {
        replication_config_input input{};
        input.handshake_socket = "/tmp/hs.sock";
        input.service_id = value;
        auto result = make_replication_config(input);
        EXPECT_FALSE(result.ok) << "value: " << value;
        EXPECT_NE(result.error_message.find("reserved"), std::string::npos) << "value: " << value;
    }
}

TEST_F(replication_config_test, boundary_service_ids_are_accepted) {
    for (std::uint64_t value : {std::uint64_t{1}, std::numeric_limits<std::uint64_t>::max() - 1}) {
        replication_config_input input{};
        input.handshake_socket = "/tmp/hs.sock";
        input.service_id = value;
        auto result = make_replication_config(input);
        EXPECT_TRUE(result.ok) << "value: " << value;
        EXPECT_EQ(result.config.service_id(), value);
    }
}

#else // LIMESTONE_ENABLE_RDMA

TEST_F(replication_config_test, rdma_mode_is_rejected_without_rdma_build) {
    replication_config_input input{};
    input.handshake_socket = "/tmp/hs.sock";
    input.service_id = 100;
    auto result = make_replication_config(input);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("ENABLE_RDMA=OFF"), std::string::npos);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

#endif // LIMESTONE_ENABLE_RDMA

// --- parse_service_id ---

TEST_F(replication_config_test, parse_service_id_accepts_plain_decimal) {
    EXPECT_EQ(parse_service_id("1"), std::uint64_t{1});
    EXPECT_EQ(parse_service_id("100"), std::uint64_t{100});
    EXPECT_EQ(parse_service_id("18446744073709551615"),
        std::numeric_limits<std::uint64_t>::max());
}

TEST_F(replication_config_test, parse_service_id_rejects_malformed_input) {
    for (char const* value : {"", "abc", "12x", " 12", "12 ", "+12", "-1", "0x10",
            "18446744073709551616"}) {
        EXPECT_EQ(parse_service_id(value), std::nullopt) << "value: '" << value << "'";
    }
}

} // namespace limestone::testing
