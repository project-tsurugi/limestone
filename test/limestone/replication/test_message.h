/*
 * Copyright 2025-2026 Project Tsurugi.
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
#pragma once

#include "replication/replication_message.h"

namespace limestone::replication {

// A test class for replication_message
class test_message : public replication_message {
public:
    // Implement the send_body method (serialization)
    void send_body(replication_message_io& io) const override {
        // For test purposes, we'll just write a simple string as message data
        io.send_string("Test Message Data");
    }

    // Implement the receive_body method (deserialization)
    void receive_body(replication_message_io& io) override {
        data = io.receive_string();
    }

    // Return the message type ID
    message_type_id get_message_type_id() const override {
        return message_type_id::TESTING;
    }

    // Factory method to create an instance of test_message
    static std::unique_ptr<replication_message> create() {
        return std::make_unique<test_message>();
    }

    // Process the message after it has been received.
    void post_receive(handler_resources& /*resources*/) override {
        // For testing, simply update the data to indicate processing.
        data = "Processed " + data;
        post_receive_called = true;
    }

    std::string get_data() const {
        return data;
    }

    inline static bool post_receive_called = false;
private:
    // inline static variable: definition inside the class to ensure single definition across translation units
    inline static const bool registered = [](){
        replication_message::register_message_type(message_type_id::TESTING, &test_message::create);
        return true;
    }();

    std::string data = "Initial Data";
};

// Static variable to ensure registration happens only once

}  // namespace limestone::replication
