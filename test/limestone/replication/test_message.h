#pragma once

#include "replication/replication_message.h"

namespace limestone::replication {

// A test class for replication_message
class test_message : public replication_message {
public:
    // Implement the send_body method (serialization)
    void send_body(socket_io& io) const override {
        // For test purposes, we'll just write a simple string as message data
        io.send_string("Test Message Data");
    }

    // Implement the receive_body method (deserialization)
    void receive_body(socket_io& io) override {
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
