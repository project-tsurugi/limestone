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

#include "replication/session_begin_message.h"
#include "gtest/gtest.h"
#include "replication/network_io.h"

namespace limestone::testing {

using limestone::replication::network_io;

// Test default send_body produces expected fields
TEST(session_begin_message_test, default_body_serialization) {
    replication::session_begin_message msg;

    std::ostringstream oss;
    msg.send_body(oss);

    std::istringstream iss(oss.str());
    uint8_t connection_type = network_io::receive_uint8(iss);
    uint64_t protocol_version = network_io::receive_uint64(iss);
    std::string configuration_id = network_io::receive_string(iss);
    uint64_t epoch_number = network_io::receive_uint64(iss);

    EXPECT_EQ(connection_type, replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(protocol_version, replication::protocol_version);
    EXPECT_EQ(configuration_id, "");
    EXPECT_EQ(epoch_number, 0u);
}

// Test set_param affects internal state via getters
TEST(session_begin_message_test, set_param_getters) {
    replication::session_begin_message msg;
    msg.set_param("config123", 42);

    EXPECT_EQ(msg.get_configuration_id(), "config123");
    EXPECT_EQ(msg.get_epoch_number(), 42u);
    EXPECT_EQ(msg.get_connection_type(), replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(msg.get_protocol_version(), replication::protocol_version);
}

// Test get_message_type_id returns SESSION_BEGIN
TEST(session_begin_message_test, get_message_type_id) {
    replication::session_begin_message msg;
    EXPECT_EQ(msg.get_message_type_id(), replication::message_type_id::SESSION_BEGIN);
}

// Test integration via replication_message::send/receive using getters to validate internal state
TEST(session_begin_message_test, replication_message_round_trip) {
    replication::session_begin_message original;
    original.set_param("roundtrip", 100);

    std::ostringstream oss;
    replication::replication_message::send(oss, original);

    std::istringstream iss(oss.str());
    auto received_base = replication::replication_message::receive(iss);
    auto received = dynamic_cast<replication::session_begin_message*>(received_base.get());
    ASSERT_NE(received, nullptr);

    // Validate internal state using getters
    EXPECT_EQ(received->get_connection_type(), replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(received->get_protocol_version(), replication::protocol_version);
    EXPECT_EQ(received->get_configuration_id(), "roundtrip");
    EXPECT_EQ(received->get_epoch_number(), 100u);
}

}  // namespace limestone::testing
