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

 #include "replication/control_channel_handler.h"
 #include "replication/message_session_begin.h"
 #include "replication/message_session_begin_ack.h"
 #include "replication/message_ack.h"
 #include "replication/socket_io.h"
 #include "gtest/gtest.h"
 
 namespace limestone::testing {
 
 using namespace limestone::replication;
 
 class dummy_server  {};
 
 class testable_control_handler : public control_channel_handler {
    public:
        using control_channel_handler::control_channel_handler;
    
        validation_result call_validate(std::unique_ptr<replication_message> m) {
            return validate_initial(std::move(m));
        }
    
        validation_result call_assign() {
            return authorize();
        }
    
        void call_send_initial_ack(socket_io& io) const {
            send_initial_ack(io);
        }
    };
 
 TEST(control_channel_handler_test, validate_session_begin_success) {
     dummy_server server;
     testable_control_handler handler(reinterpret_cast<replica_server&>(server));
 
     auto msg = std::make_unique<message_session_begin>();
     msg->set_param("conf", 1);
 
     auto result = handler.call_validate(std::move(msg));
     EXPECT_TRUE(result.ok());
 }
 
 TEST(control_channel_handler_test, assign_fails_on_second_call) {
    dummy_server server;
    testable_control_handler handler(reinterpret_cast<replica_server&>(server));

    // First call succeeds
    auto result1 = handler.call_assign();
    EXPECT_TRUE(result1.ok());

    // Second call fails (SESSION_BEGIN is considered already received)
    auto result2 = handler.call_assign();
    EXPECT_FALSE(result2.ok());
    EXPECT_EQ(result2.error_code(), 1);
}

TEST(control_channel_handler_test, validate_succeeds_after_assign) {
    dummy_server server;
    testable_control_handler handler(reinterpret_cast<replica_server&>(server));

    EXPECT_TRUE(handler.call_assign().ok());

    auto msg = std::make_unique<message_session_begin>();
    msg->set_param("conf", 42);  // 必要に応じて
    auto result = handler.call_validate(std::move(msg));

    EXPECT_TRUE(result.ok());
}

 
 TEST(control_channel_handler_test, validate_fails_on_wrong_type) {
     dummy_server server;
     testable_control_handler handler(reinterpret_cast<replica_server&>(server));
 
     auto wrong = std::make_unique<message_ack>();
     auto result = handler.call_validate(std::move(wrong));
     EXPECT_FALSE(result.ok());
     EXPECT_EQ(result.error_code(), 2);
 }
 
 TEST(control_channel_handler_test, validate_fails_on_failed_cast) {
     dummy_server server;
     testable_control_handler handler(reinterpret_cast<replica_server&>(server));
 
     class bad_message : public replication_message {
         message_type_id get_message_type_id() const override {
             return message_type_id::SESSION_BEGIN;
         }
         void send_body(socket_io&) const override {}
         void receive_body(socket_io&) override {}
         void post_receive(handler_resources&) override {}
     };
 
     auto msg = std::make_unique<bad_message>();
     auto result = handler.call_validate(std::move(msg));
     EXPECT_FALSE(result.ok());
     EXPECT_EQ(result.error_code(), 3);
 }
 
 TEST(control_channel_handler_test, send_initial_ack_outputs_session_secret) {
     dummy_server server;
     testable_control_handler handler(reinterpret_cast<replica_server&>(server));
     socket_io io("");
 
     handler.call_send_initial_ack(io);
 
     socket_io reader(io.get_out_string());
     auto msg = replication_message::receive(reader);
     auto* ack = dynamic_cast<message_session_begin_ack*>(msg.get());
     ASSERT_NE(ack, nullptr);
     EXPECT_EQ(ack->get_session_secret(), "server_.get_session_secret()");
 }
 
 }  // namespace limestone::testing