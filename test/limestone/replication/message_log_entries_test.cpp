#include "replication/message_log_entries.h"

#include <gtest/gtest.h>

#include "blob_file_resolver.h"
#include "log_entry.h"
#include "replication/blob_socket_io.h"
#include "replication/socket_io.h"

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::log_entry;

TEST(message_log_entries_test, serialize_and_deserialize_log_entries) {
    // original message with entries
    message_log_entries original{100};
    original.add_normal_entry(1, "key1", "value1", {100, 1});
    original.add_normal_entry(2, "key2", "value2", {200, 2});

    // prepare blob resolver and blob_socket_io for string mode
    const char* tmp_dir = "/tmp/message_log_entries_test";
    system((std::string("mkdir -p ") + tmp_dir).c_str());
    internal::blob_file_resolver resolver(tmp_dir);
    blob_socket_io blob_out("", resolver);

    // serialize using generic interface
    replication_message::send(blob_out, original);
    std::string wire = blob_out.get_out_string();

    // deserialize using same interface
    blob_socket_io blob_in(wire, resolver);
    std::unique_ptr<replication_message> result = replication_message::receive(blob_in);

    ASSERT_EQ(result->get_message_type_id(), message_type_id::LOG_ENTRY);
    auto* casted = dynamic_cast<message_log_entries*>(result.get());
    ASSERT_NE(casted, nullptr);

    const auto& entries = casted->get_entries();
    ASSERT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].type, api::log_entry::entry_type::normal_entry);
    EXPECT_EQ(entries[1].type, api::log_entry::entry_type::normal_entry);
}


TEST(message_log_entries_test, serialize_and_deserialize_empty_entries) {
    message_log_entries original{100};

    socket_io out("");
    replication_message::send(out, original);
    std::string data = out.get_out_string();

    socket_io in(data);
    std::unique_ptr<replication_message> received = replication_message::receive(in);

    ASSERT_EQ(received->get_message_type_id(), message_type_id::LOG_ENTRY);

    auto* casted = dynamic_cast<message_log_entries*>(received.get());
    ASSERT_NE(casted, nullptr);
    EXPECT_TRUE(casted->get_entries().empty());
}


TEST(message_log_entries_test, create_message_via_factory) {
    auto msg = message_log_entries::create();
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->get_message_type_id(), message_type_id::LOG_ENTRY);
}

}  // namespace limestone::testing
