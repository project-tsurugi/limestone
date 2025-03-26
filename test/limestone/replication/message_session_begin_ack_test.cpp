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

 #include "replication/message_session_begin_ack.h"
 #include "replication/replication_message.h"
 #include "replication/socket_io.h"
 #include "gtest/gtest.h"
 
 namespace limestone::testing {
 using namespace limestone::replication;
 
 TEST(message_session_begin_ack_test, round_trip) {
     message_session_begin_ack original;
     original.set_session_secret("secret_value");
 
     socket_io out("");
     replication_message::send(out, original);
 
     socket_io in(out.get_out_string());
     auto received_ptr = replication_message::receive(in);
     auto *received = dynamic_cast<message_session_begin_ack*>(received_ptr.get());
     ASSERT_NE(received, nullptr);
     EXPECT_EQ(received->get_session_secret(), "secret_value");
 }
 
 TEST(message_session_begin_ack_test, invalid_response_type_throws) {
     socket_io out("");
     out.send_uint16(static_cast<uint16_t>(message_type_id::SESSION_BEGIN_ACK));
     out.send_uint8(0xFF);
 
     socket_io in(out.get_out_string());
     EXPECT_THROW(replication_message::receive(in), std::runtime_error);
 }

 TEST(message_session_begin_ack_test, post_receive_throws) {
    message_session_begin_ack msg;
    msg.set_session_secret("dummy");
    EXPECT_THROW(msg.post_receive(), std::logic_error);
}

 
 } // namespace limestone::testing
 