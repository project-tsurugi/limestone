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
 
 #include "replication/message_log_channel_create.h"
 #include "replication/replication_message.h"
 #include "replication/socket_io.h"
 #include "gtest/gtest.h"
 
 namespace limestone::testing {
 using namespace limestone::replication;
 
 TEST(message_log_channel_create_test, default_values) {
     message_log_channel_create msg;
     EXPECT_EQ(msg.get_connection_type(), CONNECTION_TYPE_LOG_CHANNEL);
     EXPECT_EQ(msg.get_secret(), "");
 }
 
 TEST(message_log_channel_create_test, set_param_and_getters) {
     message_log_channel_create msg;
     msg.set_secret("config_secret");
     EXPECT_EQ(msg.get_connection_type(), CONNECTION_TYPE_LOG_CHANNEL);
     EXPECT_EQ(msg.get_secret(), "config_secret");
 }
 
 TEST(message_log_channel_create_test, replication_message_round_trip) {
     message_log_channel_create original;
     original.set_secret("roundtrip_secret");
 
     socket_io out("");
     replication_message::send(out, original);
 
     socket_io in(out.get_out_string());
     auto received_base = replication_message::receive(in);
     auto *received = dynamic_cast<message_log_channel_create*>(received_base.get());
     ASSERT_NE(received, nullptr);
     EXPECT_EQ(received->get_connection_type(), CONNECTION_TYPE_LOG_CHANNEL);
     EXPECT_EQ(received->get_secret(), "roundtrip_secret");
 }
 
 TEST(message_log_channel_create_test, invalid_connection_type_throws) {
     socket_io out("");
     out.send_uint16(static_cast<uint16_t>(message_type_id::LOG_CHANNEL_CREATE));
     out.send_uint8(CONNECTION_TYPE_CONTROL_CHANNEL);
     out.send_string("wrong_secret");
 
     socket_io in(out.get_out_string());
     EXPECT_THROW(replication_message::receive(in), std::runtime_error);
 }
 
 }  // namespace limestone::testing
 