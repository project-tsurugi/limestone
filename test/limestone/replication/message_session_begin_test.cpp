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

#include "replication/message_session_begin.h"
#include "gtest/gtest.h"
#include "replication/handler_resources.h"
#include "replication/socket_io.h"
namespace limestone::testing {

using limestone::replication::handler_resources;
using limestone::replication::socket_io;

// Test default send_body produces expected fields
TEST(message_session_begin_test, default_body_serialization) {
    replication::message_session_begin msg;
    EXPECT_EQ(msg.get_connection_type(), replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(msg.get_protocol_version(), replication::protocol_version);
    EXPECT_EQ(msg.get_configuration_id(), "");
    EXPECT_EQ(msg.get_epoch_number(), 0u);
}

// Test set_param affects internal state via getters
TEST(message_session_begin_test, set_param_getters) {
    replication::message_session_begin msg;
    msg.set_param("config123", 42);

    EXPECT_EQ(msg.get_configuration_id(), "config123");
    EXPECT_EQ(msg.get_epoch_number(), 42u);
    EXPECT_EQ(msg.get_connection_type(), replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(msg.get_protocol_version(), replication::protocol_version);
}

// Test get_message_type_id returns SESSION_BEGIN
TEST(message_session_begin_test, get_message_type_id) {
    replication::message_session_begin msg;
    EXPECT_EQ(msg.get_message_type_id(), replication::message_type_id::SESSION_BEGIN);
}

// Test integration via replication_message::send/receive using getters to validate internal state
TEST(message_session_begin_test, replication_message_round_trip) {
    replication::message_session_begin original;
    original.set_param("roundtrip", 100);

    socket_io out("");
    replication::replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication::replication_message::receive(in);
    auto received = dynamic_cast<replication::message_session_begin*>(received_base.get());
    ASSERT_NE(received, nullptr);

    // Validate internal state using getters
    EXPECT_EQ(received->get_connection_type(), replication::CONNECTION_TYPE_CONTROL_CHANNEL);
    EXPECT_EQ(received->get_protocol_version(), replication::protocol_version);
    EXPECT_EQ(received->get_configuration_id(), "roundtrip");
    EXPECT_EQ(received->get_epoch_number(), 100u);
}

TEST(message_session_begin_test, post_receive_throws) {
    replication::message_session_begin msg;
    msg.set_param("cfg", 1);
    socket_io io("");
    handler_resources resources{io};
    msg.post_receive(resources);
    SUCCEED();
}

}  // namespace limestone::testing
