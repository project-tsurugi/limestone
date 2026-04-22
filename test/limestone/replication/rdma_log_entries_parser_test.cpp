#include "replication/rdma_log_entries_parser.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <string>

#include "gtest/gtest.h"
#include "replication/blob_socket_io.h"
#include "replication/message_log_entries.h"
#include "replication/socket_io.h"
#include "test_root.h"

namespace limestone::testing {

using namespace limestone::replication;

namespace {

std::string serialize_body(message_log_entries const& message) {
    socket_io io("");
    message.send_body(io);
    return io.get_out_string();
}

std::size_t consume_one_byte_at_a_time(rdma_log_entries_parser& parser, std::string const& bytes) {
    std::size_t consumed = 0;
    while (consumed < bytes.size()
            && parser.get_status() == rdma_log_entries_parser::status::reading) {
        consumed += parser.consume(std::string_view{bytes}.substr(consumed, 1));
    }
    return consumed;
}

}  // namespace

TEST(rdma_log_entries_parser_test, parses_blobless_entries_incrementally) {
    // BLOB-less LOG_ENTRY bodies can be fully parsed by the RDMA parser.
    // Feeding one byte at a time forces every scalar, string length, and string
    // body field to cross consume() boundaries, exercising partial-field resume.
    message_log_entries original{321};
    original.add_normal_entry(1, "key1", "value1", {11, 12});
    original.add_remove_entry(2, "key2", {21, 22});
    original.add_clear_storage(3, {31, 32});
    original.set_session_begin_flag(true);
    original.set_flush_flag(true);

    std::string body = serialize_body(original);

    rdma_log_entries_parser parser;
    EXPECT_EQ(consume_one_byte_at_a_time(parser, body), body.size());
    ASSERT_TRUE(parser.complete());

    auto received = parser.take_message();
    EXPECT_EQ(received->get_epoch_id(), 321);
    EXPECT_TRUE(received->has_session_begin_flag());
    EXPECT_FALSE(received->has_session_end_flag());
    EXPECT_TRUE(received->has_flush_flag());

    ASSERT_EQ(received->get_entries().size(), 3u);
    EXPECT_EQ(received->get_entries()[0].type, log_entry::entry_type::normal_entry);
    EXPECT_EQ(received->get_entries()[0].storage_id, 1u);
    EXPECT_EQ(received->get_entries()[0].key, "key1");
    EXPECT_EQ(received->get_entries()[0].value, "value1");
    EXPECT_TRUE(received->get_entries()[0].blob_ids.empty());
    EXPECT_EQ(received->get_entries()[1].type, log_entry::entry_type::remove_entry);
    EXPECT_EQ(received->get_entries()[1].storage_id, 2u);
    EXPECT_EQ(received->get_entries()[1].key, "key2");
    EXPECT_EQ(received->get_entries()[2].type, log_entry::entry_type::clear_storage);
    EXPECT_EQ(received->get_entries()[2].storage_id, 3u);
}

TEST(rdma_log_entries_parser_test, parses_all_blobless_entry_types) {
    // Exercise every non-BLOB entry branch in add_current_entry().  BLOB entries
    // intentionally stop before add_current_entry() until BLOB receive handling
    // supplies the final blob_ids.
    message_log_entries original{322};
    original.add_normal_entry(1, "normal-key", "normal-value", {10, 11});
    original.add_remove_entry(2, "remove-key", {20, 21});
    original.add_clear_storage(3, {30, 31});
    original.add_add_storage(4, {40, 41});
    original.add_remove_storage(5, {50, 51});

    rdma_log_entries_parser parser;
    std::string body = serialize_body(original);
    EXPECT_EQ(parser.consume(body), body.size());
    ASSERT_TRUE(parser.complete());

    auto received = parser.take_message();
    ASSERT_EQ(received->get_entries().size(), 5u);
    EXPECT_EQ(received->get_entries()[0].type, log_entry::entry_type::normal_entry);
    EXPECT_EQ(received->get_entries()[1].type, log_entry::entry_type::remove_entry);
    EXPECT_EQ(received->get_entries()[2].type, log_entry::entry_type::clear_storage);
    EXPECT_EQ(received->get_entries()[3].type, log_entry::entry_type::add_storage);
    EXPECT_EQ(received->get_entries()[4].type, log_entry::entry_type::remove_storage);
}

TEST(rdma_log_entries_parser_test, stops_after_blob_count_without_consuming_blob_bytes) {
    // A BLOB entry is not complete after its fixed fields.  The parser must stop
    // immediately after blob_count, report how many BLOBs are pending, and leave
    // the BLOB header/body bytes for the later BLOB receive path.
    static constexpr const char* base_location = "/tmp/rdma_log_entries_parser_test";
    boost::filesystem::remove_all(base_location);

    limestone::api::configuration conf{};
    conf.set_data_location(base_location);
    limestone::api::datastore_test datastore{conf};

    constexpr limestone::api::blob_id_type blob_id = 987;
    auto path = datastore.get_blob_file(blob_id).path();
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream(path.string(), std::ios::binary) << "blob-body";

    message_log_entries original{654};
    original.add_normal_with_blob(3, "key", "value", {31, 32}, {blob_id});
    original.set_session_end_flag(true);

    blob_socket_io io("", datastore);
    original.send_body(io);
    std::string body = io.get_out_string();

    rdma_log_entries_parser parser;
    std::size_t consumed = parser.consume(body);

    EXPECT_TRUE(parser.awaiting_blob());
    EXPECT_EQ(parser.get_status(), rdma_log_entries_parser::status::awaiting_blob);
    EXPECT_EQ(parser.pending_blob_count(), 1u);
    EXPECT_EQ(parser.entries_remaining(), 1u);
    EXPECT_LT(consumed, body.size());

    boost::filesystem::remove_all(base_location);
}

TEST(rdma_log_entries_parser_test, complete_message_leaves_trailing_bytes_unconsumed) {
    // RDMA payloads may eventually contain bytes after the current LOG_ENTRY
    // body.  consume() must stop at message completion and report only the bytes
    // consumed by this message, leaving trailing bytes for the caller.
    message_log_entries original{123};
    original.add_normal_entry(1, "key", "value", {2, 3});

    std::string body = serialize_body(original);
    std::string bytes = body + "trailing";

    rdma_log_entries_parser parser;
    EXPECT_EQ(parser.consume(bytes), body.size());
    EXPECT_TRUE(parser.complete());
    EXPECT_EQ(parser.get_status(), rdma_log_entries_parser::status::complete);
    EXPECT_EQ(parser.consume("more trailing bytes"), 0u);
}

TEST(rdma_log_entries_parser_test, take_message_resets_parser_for_next_message) {
    // take_message() is also the parser reuse boundary.  After a completed
    // message is taken, the same parser instance must be able to parse a fresh
    // LOG_ENTRY body without carrying over state from the previous one.
    message_log_entries first{100};
    first.add_normal_entry(1, "first-key", "first-value", {10, 11});

    message_log_entries second{200};
    second.add_remove_entry(2, "second-key", {20, 21});
    second.set_flush_flag(true);

    rdma_log_entries_parser parser;
    std::string first_body = serialize_body(first);
    EXPECT_EQ(parser.consume(first_body), first_body.size());
    ASSERT_TRUE(parser.complete());

    auto first_received = parser.take_message();
    EXPECT_EQ(first_received->get_epoch_id(), 100);
    ASSERT_EQ(first_received->get_entries().size(), 1u);
    EXPECT_EQ(first_received->get_entries()[0].key, "first-key");

    std::string second_body = serialize_body(second);
    EXPECT_EQ(parser.consume(second_body), second_body.size());
    ASSERT_TRUE(parser.complete());

    auto second_received = parser.take_message();
    EXPECT_EQ(second_received->get_epoch_id(), 200);
    EXPECT_TRUE(second_received->has_flush_flag());
    ASSERT_EQ(second_received->get_entries().size(), 1u);
    EXPECT_EQ(second_received->get_entries()[0].type, log_entry::entry_type::remove_entry);
    EXPECT_EQ(second_received->get_entries()[0].key, "second-key");
}

TEST(rdma_log_entries_parser_test, partial_scalar_keeps_reading_status_until_complete) {
    // Fixed-size scalar fields may be split across RDMA frames.  Supplying only
    // seven bytes of the initial uint64 epoch_id must keep the parser in
    // reading state; the eighth byte should complete the scalar and allow the
    // parser to continue.
    message_log_entries original{0x0102030405060708ULL};
    std::string body = serialize_body(original);

    rdma_log_entries_parser parser;
    ASSERT_GE(body.size(), 8u);

    EXPECT_EQ(parser.consume(std::string_view{body}.substr(0, 7)), 7u);
    EXPECT_EQ(parser.get_status(), rdma_log_entries_parser::status::reading);
    EXPECT_FALSE(parser.complete());

    EXPECT_EQ(parser.consume(std::string_view{body}.substr(7)), body.size() - 7);
    ASSERT_TRUE(parser.complete());

    auto received = parser.take_message();
    EXPECT_EQ(received->get_epoch_id(), 0x0102030405060708ULL);
    EXPECT_TRUE(received->get_entries().empty());
}

TEST(rdma_log_entries_parser_test, consume_returns_zero_while_awaiting_blob) {
    // Once awaiting_blob is reached, this parser must not consume further bytes
    // until the BLOB receive path advances it.
    static constexpr const char* base_location = "/tmp/rdma_log_entries_parser_awaiting_blob_test";
    boost::filesystem::remove_all(base_location);

    limestone::api::configuration conf{};
    conf.set_data_location(base_location);
    limestone::api::datastore_test datastore{conf};

    constexpr limestone::api::blob_id_type blob_id = 4321;
    auto path = datastore.get_blob_file(blob_id).path();
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream(path.string(), std::ios::binary) << "blob-body";

    message_log_entries original{790};
    original.add_normal_with_blob(3, "key", "value", {4, 5}, {blob_id});

    blob_socket_io io("", datastore);
    original.send_body(io);

    rdma_log_entries_parser parser;
    std::string body = io.get_out_string();
    ASSERT_LT(parser.consume(body), body.size());
    ASSERT_TRUE(parser.awaiting_blob());

    EXPECT_EQ(parser.consume("ignored"), 0u);
    EXPECT_TRUE(parser.awaiting_blob());

    boost::filesystem::remove_all(base_location);
}

TEST(rdma_log_entries_parser_test, parses_empty_key_and_value) {
    // Strings are encoded as length followed by body bytes.  A zero-length key
    // or value has no body bytes, so the parser must advance immediately from
    // the length field to the next state and preserve the empty string.
    message_log_entries original{456};
    original.add_normal_entry(7, "", "", {8, 9});

    rdma_log_entries_parser parser;
    std::string body = serialize_body(original);
    EXPECT_EQ(consume_one_byte_at_a_time(parser, body), body.size());
    ASSERT_TRUE(parser.complete());

    auto received = parser.take_message();
    ASSERT_EQ(received->get_entries().size(), 1u);
    EXPECT_EQ(received->get_entries()[0].storage_id, 7u);
    EXPECT_TRUE(received->get_entries()[0].key.empty());
    EXPECT_TRUE(received->get_entries()[0].value.empty());
}

TEST(rdma_log_entries_parser_test, blob_entry_leaves_blob_body_and_following_bytes_unconsumed) {
    // Stronger BLOB-boundary check: even if BLOB header/body bytes and arbitrary
    // following bytes are present in the same input buffer, this parser must
    // stop at blob_count.  The next unread byte should still be the beginning of
    // the BLOB wire data for the later BLOB receive path.
    static constexpr const char* base_location = "/tmp/rdma_log_entries_parser_blob_tail_test";
    boost::filesystem::remove_all(base_location);

    limestone::api::configuration conf{};
    conf.set_data_location(base_location);
    limestone::api::datastore_test datastore{conf};

    constexpr limestone::api::blob_id_type blob_id = 1234;
    auto path = datastore.get_blob_file(blob_id).path();
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream(path.string(), std::ios::binary) << "blob-body";

    message_log_entries original{789};
    original.add_normal_with_blob(3, "key", "value", {4, 5}, {blob_id});

    blob_socket_io io("", datastore);
    original.send_body(io);
    std::string body = io.get_out_string();
    std::string bytes = body + "following";

    rdma_log_entries_parser parser;
    std::size_t consumed = parser.consume(bytes);

    EXPECT_TRUE(parser.awaiting_blob());
    EXPECT_EQ(parser.pending_blob_count(), 1u);
    EXPECT_LT(consumed, body.size());
    EXPECT_EQ(bytes.substr(consumed, sizeof(blob_id)), body.substr(consumed, sizeof(blob_id)));

    boost::filesystem::remove_all(base_location);
}

TEST(rdma_log_entries_parser_test, parses_empty_message_and_operation_flags) {
    // entry_count == 0 is a distinct state transition: after reading the count,
    // the parser should skip entry parsing, read operation_flags, and complete.
    message_log_entries original{777};
    original.set_session_end_flag(true);

    std::string body = serialize_body(original);

    rdma_log_entries_parser parser;
    EXPECT_EQ(parser.consume(body), body.size());
    ASSERT_TRUE(parser.complete());

    auto received = parser.take_message();
    EXPECT_EQ(received->get_epoch_id(), 777);
    EXPECT_TRUE(received->get_entries().empty());
    EXPECT_FALSE(received->has_session_begin_flag());
    EXPECT_TRUE(received->has_session_end_flag());
    EXPECT_FALSE(received->has_flush_flag());
}

}  // namespace limestone::testing
