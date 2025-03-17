#pragma once

#include "replication/replication_message.h"

namespace limestone::replication {

// A test class for replication_message
class test_message : public replication_message {
public:
    // Implement the send_body method (serialization)
    void send_body(std::ostream& os) const override {
        // For test purposes, we'll just write a simple string as message data
        os << "Test Message Data";
    }

    // Implement the receive_body method (deserialization)
    void receive_body(std::istream& is) override {
        is >> data;
        // For test purposes, just read the data
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
    void post_receive() override {
        // For testing, simply update the data to indicate processing.
        data = "Processed " + data;
    }

    // Retrieve internal data for testing purposes.
    std::string get_data_for_testing() const override {
        return data;
    }

private:
    // Static variable to ensure registration happens only once
    static const bool registered;

    std::string data = "Initial Data";
};

// Static variable to ensure registration happens only once
const bool test_message::registered = []() {
    // Register the message type with the message map
    replication_message::register_message_type(message_type_id::TESTING, &test_message::create);
    return true;
}();

}  // namespace limestone::replication
