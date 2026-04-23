#include "replication/rdma_log_entries_receiver.h"

#include <boost/filesystem.hpp>

#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "replication/blob_socket_io.h"
#include "replication/replication_message.h"
#include "replication/socket_io.h"
#include "test_root.h"

namespace limestone::testing {

using namespace limestone::replication;

namespace {

std::string serialize_message(replication_message const& message) {
    socket_io io("");
    replication_message::send(io, message);
    return io.get_out_string();
}

std::string serialize_message_with_blobs(replication_message const& message, limestone::api::datastore& datastore) {
    blob_socket_io io("", datastore);
    replication_message::send(io, message);
    return io.get_out_string();
}

std::string read_file(boost::filesystem::path const& path) {
    std::ifstream in(path.string(), std::ios::binary);
    if (!in.is_open()) {
        ADD_FAILURE() << "Failed to open file: " << path.string();
        return {};
    }

    std::string content{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    if (in.bad()) {
        ADD_FAILURE() << "Failed to read file: " << path.string();
        return {};
    }
    return content;
}

}  // namespace

TEST(rdma_log_entries_receiver_test, parses_blobless_message_split_after_type) {
    // The receiver owns the replication message type byte.  This test feeds the
    // type alone first, then the body one byte at a time, so both the receiver
    // state and the delegated LOG_ENTRY body parser have to resume correctly.
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_blobless_test";
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    message_log_entries original{1001};
    original.add_normal_entry(1, "key", "value", {2, 3});
    original.set_flush_flag(true);
    std::string bytes = serialize_message(original);

    rdma_log_entries_receiver receiver{receiver_datastore};
    ASSERT_FALSE(bytes.empty());
    EXPECT_EQ(receiver.consume(std::string_view{bytes}.substr(0, 1)), 1u);
    EXPECT_TRUE(receiver.reading_message());
    EXPECT_FALSE(receiver.has_message());

    std::size_t consumed = 1;
    while (consumed < bytes.size()) {
        consumed += receiver.consume(std::string_view{bytes}.substr(consumed, 1));
    }

    ASSERT_TRUE(receiver.has_message());
    EXPECT_EQ(receiver.message_count(), 1u);
    EXPECT_FALSE(receiver.reading_message());

    auto received = receiver.take_message();
    EXPECT_EQ(received->get_epoch_id(), 1001);
    EXPECT_TRUE(received->has_flush_flag());
    ASSERT_EQ(received->get_entries().size(), 1u);
    EXPECT_EQ(received->get_entries()[0].type, log_entry::entry_type::normal_entry);
    EXPECT_EQ(received->get_entries()[0].key, "key");
    EXPECT_EQ(received->get_entries()[0].value, "value");
    EXPECT_FALSE(receiver.has_message());

    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, parses_multiple_messages_in_one_input) {
    // One RDMA payload can contain more than one serialized LOG_ENTRY message.
    // The receiver must stop each body at its operation_flags byte, take the
    // completed message from the body parser, and immediately continue with the
    // next message type byte in the same input buffer.
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_multi_message_test";
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    message_log_entries first{1101};
    first.add_normal_entry(1, "first", "value1", {2, 3});
    message_log_entries second{1102};
    second.add_remove_entry(2, "second", {4, 5});
    second.set_session_end_flag(true);

    std::string bytes = serialize_message(first) + serialize_message(second);

    rdma_log_entries_receiver receiver{receiver_datastore};
    EXPECT_EQ(receiver.consume(bytes), bytes.size());
    ASSERT_EQ(receiver.message_count(), 2u);

    auto first_received = receiver.take_message();
    EXPECT_EQ(first_received->get_epoch_id(), 1101);
    ASSERT_EQ(first_received->get_entries().size(), 1u);
    EXPECT_EQ(first_received->get_entries()[0].key, "first");

    auto second_received = receiver.take_message();
    EXPECT_EQ(second_received->get_epoch_id(), 1102);
    EXPECT_TRUE(second_received->has_session_end_flag());
    ASSERT_EQ(second_received->get_entries().size(), 1u);
    EXPECT_EQ(second_received->get_entries()[0].type, log_entry::entry_type::remove_entry);
    EXPECT_EQ(second_received->get_entries()[0].key, "second");
    EXPECT_FALSE(receiver.has_message());

    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, streams_blob_message_split_across_inputs) {
    // BLOB body bytes are handled by the delegated parser with the receiver's
    // datastore.  Splitting the serialized message confirms that the receiver
    // does not need the full RDMA payload in memory before BLOB restore starts.
    static constexpr const char* sender_location = "/tmp/rdma_log_entries_receiver_blob_sender_test";
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_blob_receiver_test";
    boost::filesystem::remove_all(sender_location);
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_location);
    limestone::api::datastore_test sender_datastore{sender_conf};

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    constexpr limestone::api::blob_id_type blob_id = 9876;
    std::string blob_body = "receiver-split-blob-body";
    auto sender_blob_path = sender_datastore.get_blob_file(blob_id).path();
    boost::filesystem::create_directories(sender_blob_path.parent_path());
    std::ofstream(sender_blob_path.string(), std::ios::binary) << blob_body;

    message_log_entries original{1201};
    original.add_normal_with_blob(3, "blob-key", "blob-value", {4, 5}, {blob_id});
    std::string bytes = serialize_message_with_blobs(original, sender_datastore);

    rdma_log_entries_receiver receiver{receiver_datastore};
    std::size_t consumed = 0;
    while (consumed < bytes.size()) {
        std::size_t chunk = consumed % 3 == 0 ? 2u : 5u;
        chunk = std::min(chunk, bytes.size() - consumed);
        consumed += receiver.consume(std::string_view{bytes}.substr(consumed, chunk));
    }

    ASSERT_TRUE(receiver.has_message());
    auto received = receiver.take_message();
    EXPECT_EQ(received->get_epoch_id(), 1201);
    ASSERT_EQ(received->get_entries().size(), 1u);
    EXPECT_EQ(received->get_entries()[0].type, log_entry::entry_type::normal_with_blob);
    EXPECT_EQ(received->get_entries()[0].key, "blob-key");
    EXPECT_EQ(received->get_entries()[0].value, "blob-value");
    EXPECT_EQ(received->get_entries()[0].blob_ids, std::vector<limestone::api::blob_id_type>{blob_id});

    auto receiver_blob_path = receiver_datastore.get_blob_file(blob_id).path();
    EXPECT_EQ(read_file(receiver_blob_path), blob_body);

    boost::filesystem::remove_all(sender_location);
    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, parses_multiple_blob_messages_in_one_session) {
    // A persistent session may send more than one LOG_ENTRY message containing
    // BLOBs.  Feed two serialized BLOB messages as one RDMA byte stream, split
    // across arbitrary consume() boundaries, and verify that both messages and
    // both replica BLOB files are completed independently.
    static constexpr const char* sender_location = "/tmp/rdma_log_entries_receiver_multi_blob_sender_test";
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_multi_blob_receiver_test";
    boost::filesystem::remove_all(sender_location);
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_location);
    limestone::api::datastore_test sender_datastore{sender_conf};

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    constexpr limestone::api::blob_id_type first_blob_id = 9877;
    constexpr limestone::api::blob_id_type second_blob_id = 9878;
    std::string const first_blob_body = "first-session-blob";
    std::string const second_blob_body = "second-session-blob";

    auto first_sender_blob_path = sender_datastore.get_blob_file(first_blob_id).path();
    boost::filesystem::create_directories(first_sender_blob_path.parent_path());
    std::ofstream(first_sender_blob_path.string(), std::ios::binary) << first_blob_body;

    auto second_sender_blob_path = sender_datastore.get_blob_file(second_blob_id).path();
    boost::filesystem::create_directories(second_sender_blob_path.parent_path());
    std::ofstream(second_sender_blob_path.string(), std::ios::binary) << second_blob_body;

    message_log_entries first{1202};
    first.set_session_begin_flag(true);
    first.add_normal_with_blob(3, "blob-key-1", "blob-value-1", {4, 5}, {first_blob_id});

    message_log_entries second{1202};
    second.set_session_end_flag(true);
    second.add_normal_with_blob(4, "blob-key-2", "blob-value-2", {4, 6}, {second_blob_id});

    std::string bytes = serialize_message_with_blobs(first, sender_datastore)
                      + serialize_message_with_blobs(second, sender_datastore);

    rdma_log_entries_receiver receiver{receiver_datastore};
    std::size_t consumed = 0;
    while (consumed < bytes.size()) {
        std::size_t chunk = consumed % 2 == 0 ? 7u : 3u;
        chunk = std::min(chunk, bytes.size() - consumed);
        consumed += receiver.consume(std::string_view{bytes}.substr(consumed, chunk));
    }

    ASSERT_EQ(receiver.message_count(), 2u);

    auto first_received = receiver.take_message();
    EXPECT_EQ(first_received->get_epoch_id(), 1202);
    EXPECT_TRUE(first_received->has_session_begin_flag());
    ASSERT_EQ(first_received->get_entries().size(), 1u);
    EXPECT_EQ(first_received->get_entries()[0].blob_ids,
            std::vector<limestone::api::blob_id_type>{first_blob_id});

    auto second_received = receiver.take_message();
    EXPECT_EQ(second_received->get_epoch_id(), 1202);
    EXPECT_TRUE(second_received->has_session_end_flag());
    ASSERT_EQ(second_received->get_entries().size(), 1u);
    EXPECT_EQ(second_received->get_entries()[0].blob_ids,
            std::vector<limestone::api::blob_id_type>{second_blob_id});

    auto first_receiver_blob_path = receiver_datastore.get_blob_file(first_blob_id).path();
    auto second_receiver_blob_path = receiver_datastore.get_blob_file(second_blob_id).path();
    EXPECT_EQ(read_file(first_receiver_blob_path), first_blob_body);
    EXPECT_EQ(read_file(second_receiver_blob_path), second_blob_body);

    boost::filesystem::remove_all(sender_location);
    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, leaves_partial_next_message_in_progress_after_completed_message) {
    // If the input ends after the next message type byte, the previous message
    // should still be available while the receiver remembers that a new message
    // body is in progress.
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_partial_next_test";
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    message_log_entries first{1301};
    first.add_normal_entry(1, "first", "value", {2, 3});
    message_log_entries second{1302};
    second.add_normal_entry(2, "second", "value", {4, 5});

    std::string first_bytes = serialize_message(first);
    std::string second_bytes = serialize_message(second);
    std::string bytes = first_bytes + second_bytes.substr(0, 1);

    rdma_log_entries_receiver receiver{receiver_datastore};
    EXPECT_EQ(receiver.consume(bytes), bytes.size());
    EXPECT_EQ(receiver.message_count(), 1u);
    EXPECT_TRUE(receiver.reading_message());

    auto first_received = receiver.take_message();
    EXPECT_EQ(first_received->get_epoch_id(), 1301);
    EXPECT_FALSE(receiver.has_message());

    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, rejects_non_log_entry_message_type) {
    // The RDMA log-channel receiver currently accepts only LOG_ENTRY messages.
    // Seeing any other replication message type is a protocol error for this
    // component.
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_invalid_type_test";
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    rdma_log_entries_receiver receiver{receiver_datastore};
    std::string bytes(1, static_cast<char>(message_type_id::COMMON_ACK));
    EXPECT_THROW([[maybe_unused]] std::size_t consumed = receiver.consume(bytes), std::runtime_error);
    EXPECT_FALSE(receiver.has_message());
    EXPECT_FALSE(receiver.reading_message());

    boost::filesystem::remove_all(receiver_location);
}

TEST(rdma_log_entries_receiver_test, take_message_without_completed_message_throws) {
    static constexpr const char* receiver_location = "/tmp/rdma_log_entries_receiver_empty_take_test";
    boost::filesystem::remove_all(receiver_location);

    limestone::api::configuration receiver_conf{};
    receiver_conf.set_data_location(receiver_location);
    limestone::api::datastore_test receiver_datastore{receiver_conf};

    rdma_log_entries_receiver receiver{receiver_datastore};
    EXPECT_THROW(receiver.take_message(), std::logic_error);

    boost::filesystem::remove_all(receiver_location);
}

}  // namespace limestone::testing
