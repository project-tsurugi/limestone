#include "replication/message_log_entries.h"

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <fstream>
#include "replication/replica_connector.h"
#include "replication/replica_server.h"
#include "replication_test_helper.h"
#include "blob_file_resolver.h"
#include "log_entry.h"
#include "replication/blob_socket_io.h"
#include "replication/socket_io.h"
#include "test_root.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_session_begin.h"
#include "replication/log_channel_handler_resources.h"

namespace limestone::testing {

using namespace limestone::replication;
using limestone::api::log_entry;


static constexpr const char* base_location = "/tmp/message_log_entries_test";
static constexpr const char* master = "/tmp/message_log_entries_test/master";
static constexpr const char* replica = "/tmp/message_log_entries_test/replica";

class message_log_entries_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
        
        limestone::api::configuration conf{};
        conf.set_data_location(replica);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);        
    }

    void TearDown() override {
        datastore_.reset();
        boost::filesystem::remove_all(base_location);
    }

    std::unique_ptr<api::datastore_test> datastore_;
};

TEST_F(message_log_entries_test, serialize_and_deserialize_log_entries) {
    message_log_entries original{100};
    original.add_normal_entry(1, "key1", "value1", {100, 1});
    original.add_normal_entry(2, "key2", "value2", {200, 2});

    blob_socket_io blob_out("", *datastore_);
    replication_message::send(blob_out, original);
    std::string wire = blob_out.get_out_string();

    blob_socket_io blob_in(wire, *datastore_);
    std::unique_ptr<replication_message> result = replication_message::receive(blob_in);

    ASSERT_EQ(result->get_message_type_id(), message_type_id::LOG_ENTRY);
    auto* casted = dynamic_cast<message_log_entries*>(result.get());
    ASSERT_NE(casted, nullptr);

    const auto& entries = casted->get_entries();
    ASSERT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].type, log_entry::entry_type::normal_entry);
    EXPECT_EQ(entries[1].type, log_entry::entry_type::normal_entry);
}

TEST_F(message_log_entries_test, serialize_and_deserialize_empty_entries) {
    message_log_entries original{100};
    socket_io out("" );
    replication_message::send(out, original);
    std::string data = out.get_out_string();

    socket_io in(data);
    std::unique_ptr<replication_message> received = replication_message::receive(in);

    ASSERT_EQ(received->get_message_type_id(), message_type_id::LOG_ENTRY);
    auto* casted = dynamic_cast<message_log_entries*>(received.get());
    ASSERT_NE(casted, nullptr);
    EXPECT_TRUE(casted->get_entries().empty());
}

TEST_F(message_log_entries_test, create_message_via_factory) {
    auto msg = message_log_entries::create();
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->get_message_type_id(), message_type_id::LOG_ENTRY);
}

TEST_F(message_log_entries_test, epoch_id_round_trip) {
    message_log_entries original{12345};
    blob_socket_io out("", *datastore_);
    replication_message::send(out, original);
    std::string wire = out.get_out_string();

    blob_socket_io in(wire, *datastore_);
    auto received = replication_message::receive(in);
    auto* msg = dynamic_cast<message_log_entries*>(received.get());
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->get_epoch_id(), 12345);
}

TEST_F(message_log_entries_test, serialize_and_deserialize_normal_with_blob) {
    // Prepare a message with one normal_with_blob entry
    message_log_entries original{42};
    limestone::api::blob_id_type blob1 = 111;
    limestone::api::blob_id_type blob2 = 222;
    std::vector<limestone::api::blob_id_type> blobs = {blob1, blob2};

    // Create dummy blob files
    auto path1 = datastore_->get_blob_file(blob1).path();
    auto path2 = datastore_->get_blob_file(blob2).path();
    boost::filesystem::create_directories(path1.parent_path());
    boost::filesystem::create_directories(path2.parent_path());
    std::ofstream(path1.string(), std::ios::binary) << "foo";
    std::ofstream(path2.string(), std::ios::binary) << "bar";

    original.add_normal_with_blob(5, "key", "value", {7, 8}, blobs);

    // Serialize
    blob_socket_io out("", *datastore_);
    replication_message::send(out, original);
    std::string wire = out.get_out_string();

    // Remove original files so receive must recreate
    boost::filesystem::remove(path1);
    boost::filesystem::remove(path2);

    // Deserialize
    blob_socket_io in(wire, *datastore_);
    auto received_msg = replication_message::receive(in);
    ASSERT_EQ(received_msg->get_message_type_id(), message_type_id::LOG_ENTRY);

    auto* casted = dynamic_cast<message_log_entries*>(received_msg.get());
    ASSERT_NE(casted, nullptr);

    const auto& entries = casted->get_entries();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(casted->get_epoch_id(), 42);

    const auto& e = entries[0];
    EXPECT_EQ(e.type, log_entry::entry_type::normal_with_blob);
    EXPECT_EQ(e.storage_id, 5u);
    EXPECT_EQ(e.key, "key");
    EXPECT_EQ(e.value, "value");
    EXPECT_EQ(e.write_version.get_major(), 7u);
    EXPECT_EQ(e.write_version.get_minor(), 8u);
    ASSERT_EQ(e.blob_ids.size(), 2u);
    EXPECT_EQ(e.blob_ids[0], blob1);
    EXPECT_EQ(e.blob_ids[1], blob2);

    // Verify files recreated with correct contents
    std::string contents;
    std::ifstream(path1.string(), std::ios::binary) >> contents;
    EXPECT_EQ(contents, "foo");
    std::ifstream(path2.string(), std::ios::binary) >> contents;
    EXPECT_EQ(contents, "bar");
}

TEST_F(message_log_entries_test, serialize_and_deserialize_various_entry_types) {
    // Prepare message with all entry types
    message_log_entries original{77};
    original.add_normal_entry(10, "normal_key", "normal_value", {1, 2});
    original.add_remove_entry(20, "remove_key", {3, 4});
    original.add_clear_storage(30, {5, 6});
    original.add_add_storage(40, {7, 8});
    original.add_remove_storage(50, {9, 10});

    blob_socket_io out("", *datastore_);
    replication_message::send(out, original);
    std::string wire = out.get_out_string();

    blob_socket_io in(wire, *datastore_);
    auto received = replication_message::receive(in);
    ASSERT_EQ(received->get_message_type_id(), message_type_id::LOG_ENTRY);

    auto* msg = dynamic_cast<message_log_entries*>(received.get());
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->get_epoch_id(), 77);

    const auto& entries = msg->get_entries();
    ASSERT_EQ(entries.size(), 5u);

    // normal_entry
    EXPECT_EQ(entries[0].type, log_entry::entry_type::normal_entry);
    EXPECT_EQ(entries[0].storage_id, 10u);
    EXPECT_EQ(entries[0].key, "normal_key");
    EXPECT_EQ(entries[0].value, "normal_value");
    EXPECT_EQ(entries[0].write_version.get_major(), 1u);
    EXPECT_EQ(entries[0].write_version.get_minor(), 2u);

    // remove_entry
    EXPECT_EQ(entries[1].type, log_entry::entry_type::remove_entry);
    EXPECT_EQ(entries[1].storage_id, 20u);
    EXPECT_EQ(entries[1].key, "remove_key");
    EXPECT_TRUE(entries[1].value.empty());

    // clear_storage
    EXPECT_EQ(entries[2].type, log_entry::entry_type::clear_storage);
    EXPECT_EQ(entries[2].storage_id, 30u);

    // add_storage
    EXPECT_EQ(entries[3].type, log_entry::entry_type::add_storage);
    EXPECT_EQ(entries[3].storage_id, 40u);

    // remove_storage
    EXPECT_EQ(entries[4].type, log_entry::entry_type::remove_storage);
    EXPECT_EQ(entries[4].storage_id, 50u);
}

TEST_F(message_log_entries_test, operation_flags_round_trip) {
    constexpr epoch_id_type k_epoch = 123;
    for (uint8_t mask = 0; mask < 8; ++mask) {
        SCOPED_TRACE("flags_mask=" + std::to_string(mask));
        message_log_entries original{k_epoch};
        original.set_session_begin_flag(mask & message_log_entries::SESSION_BEGIN_FLAG);
        original.set_session_end_flag(mask & message_log_entries::SESSION_END_FLAG);
        original.set_flush_flag(mask & message_log_entries::FLUSH_FLAG);

        socket_io out("");
        replication_message::send(out, original);
        std::string wire = out.get_out_string();

        socket_io in(wire);
        auto received = replication_message::receive(in);
        ASSERT_EQ(received->get_message_type_id(), message_type_id::LOG_ENTRY);

        auto* msg = dynamic_cast<message_log_entries*>(received.get());
        ASSERT_NE(msg, nullptr);
        EXPECT_EQ(msg->get_epoch_id(), k_epoch);
        EXPECT_EQ(msg->has_session_begin_flag(), static_cast<bool>(mask & message_log_entries::SESSION_BEGIN_FLAG));
        EXPECT_EQ(msg->has_session_end_flag(), static_cast<bool>(mask & message_log_entries::SESSION_END_FLAG));
        EXPECT_EQ(msg->has_flush_flag(), static_cast<bool>(mask & message_log_entries::FLUSH_FLAG));
    }
}

TEST_F(message_log_entries_test, write_version_round_trip) {
    constexpr epoch_id_type k_epoch = 999;
    message_log_entries original{k_epoch};

    limestone::api::write_version_type version1{11, 22};
    limestone::api::write_version_type version2{33, 44};

    original.add_normal_entry(100, "key1", "value1", version1);
    original.add_remove_entry(200, "key2", version2);

    socket_io out("");
    replication_message::send(out, original);
    std::string wire = out.get_out_string();

    socket_io in(wire);
    auto received = replication_message::receive(in);
    ASSERT_EQ(received->get_message_type_id(), message_type_id::LOG_ENTRY);

    auto* msg = dynamic_cast<message_log_entries*>(received.get());
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->get_epoch_id(), k_epoch);

    const auto& entries = msg->get_entries();
    ASSERT_EQ(entries.size(), 2u);

    EXPECT_EQ(entries[0].write_version.get_major(), 11u);
    EXPECT_EQ(entries[0].write_version.get_minor(), 22u);
    EXPECT_EQ(entries[1].write_version.get_major(), 33u);
    EXPECT_EQ(entries[1].write_version.get_minor(), 44u);
}

TEST_F(message_log_entries_test, mixed_socket_io_blob_socket_io_round_trip) {
    constexpr epoch_id_type k_epoch = 2025;
    message_log_entries original{k_epoch};

    // Case A: no blobs -> both socket_io↔blob_socket_io must work
    original.add_normal_entry(1, "k", "v", {1,1});

    {
        socket_io out("");
        replication_message::send(out, original);
        std::string wire = out.get_out_string();

        blob_socket_io in(wire, *datastore_);
        auto received = replication_message::receive(in);
        auto* msg = dynamic_cast<message_log_entries*>(received.get());
        ASSERT_NE(msg, nullptr);
        EXPECT_EQ(msg->get_epoch_id(), k_epoch);
        EXPECT_EQ(msg->get_entries().size(), 1u);
    }
    {
        blob_socket_io out("", *datastore_);
        replication_message::send(out, original);
        std::string wire = out.get_out_string();

        socket_io in(wire);
        auto received = replication_message::receive(in);
        auto* msg = dynamic_cast<message_log_entries*>(received.get());
        ASSERT_NE(msg, nullptr);
        EXPECT_EQ(msg->get_epoch_id(), k_epoch);
        EXPECT_EQ(msg->get_entries().size(), 1u);
    }

    // Case B: with blobs -> socket_io send should ASSERT_DEATH
    message_log_entries with_blobs{k_epoch};
    with_blobs.add_normal_with_blob(2, "k2", "v2", {2,2}, {42});


    EXPECT_DEATH(
        {
            socket_io out("");
            replication_message::send(out, with_blobs);
        },
        ".*"
    );
}

TEST_F(message_log_entries_test, receiving_blob_entry_with_socket_io_should_fail) {
    message_log_entries original{42};
    limestone::api::blob_id_type blob_id = 999;
    auto path = datastore_->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream(path.string(), std::ios::binary) << "dummy";

    original.add_normal_with_blob(1, "key", "value", {1, 1}, {blob_id});

    // Serialize with blob_socket_io (OK)
    blob_socket_io blob_out("", *datastore_);
    replication_message::send(blob_out, original);
    std::string wire = blob_out.get_out_string();

    // Deserialize with plain socket_io (this should fail)
    EXPECT_DEATH(
        {
            socket_io in(wire);  // intentionally wrong
            replication_message::receive(in);  // this should hit the FATAL
        },
        ".*"
    );
}

TEST_F(message_log_entries_test, post_receive) {
    auto& lc = datastore_->create_channel();
    datastore_->ready();
    socket_io io("");
    log_channel_handler_resources resources(io, lc);

    // 正常系
    message_log_entries msg{100};
    msg.set_session_begin_flag(true);
    msg.add_normal_entry(1, "key1", "value1", {100, 1});
    msg.add_normal_with_blob(2, "key2", "value2", {200, 2},{99});
    msg.add_remove_entry(3, "key3", {300, 3});
    msg.add_clear_storage(4, {400, 4});
    msg.add_add_storage(5, {500, 5});
    msg.add_remove_storage(6, {600, 6});
    msg.set_flush_flag(true);
    msg.set_session_end_flag(true);

    msg.post_receive(resources);

    auto log_entries = read_log_file(std::string(replica) + "/pwal_0000");
    EXPECT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 100, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 200, 2, {99}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 3, "key3", "", 300, 3, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 4, "", "", 400, 4, {}, log_entry::entry_type::clear_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 5, "", "", 500, 5, {}, log_entry::entry_type::add_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 6, "", "", 600, 6, {}, log_entry::entry_type::remove_storage));

    auto ack_str = io.get_out_string();
    auto ack = replication_message::receive(io);
    ASSERT_EQ(ack->get_message_type_id(), message_type_id::COMMON_ACK);
}


TEST_F(message_log_entries_test, write_all_entry_type_to_pwal_via_replication_channel) {
    // Setup datastore for master(client)
    limestone::api::configuration conf{};
    conf.set_data_location(master);
    auto ds = std::make_unique<limestone::api::datastore_test>(conf);

    // Stop the datastore_ created in SetUp because it conflicts with the replica server
    datastore_.reset();

    // Setup replica server
    replication::replica_server server;
    server.initialize(replica);

    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() {
        try {
            server.accept_loop();
        } catch (const std::exception& ex) {
            std::cerr << "Exception in server.accept_loop(): " << ex.what() << std::endl;
            throw;
        } catch (...) {
            std::cerr << "Unknown exception in server.accept_loop()." << std::endl;
            throw;
        }
    });

    // Hold all clients open until the end
    std::vector<replica_connector> clients;

    // 1) Open control session
    clients.emplace_back();
    ASSERT_TRUE(clients.back().connect_to_server("127.0.0.1", port, *ds));
    {
        auto request = message_session_begin::create();
        EXPECT_TRUE(clients.back().send_message(*request));
        auto response = clients.back().receive_message();
        ASSERT_NE(response, nullptr);
        EXPECT_EQ(response->get_message_type_id(), message_type_id::SESSION_BEGIN_ACK);
    }

    // 2) Open five log channel sessions
    clients.emplace_back();
    ASSERT_TRUE(clients.back().connect_to_server("127.0.0.1", port, *ds));
    auto request = message_log_channel_create::create();
    EXPECT_TRUE(clients.back().send_message(*request));

    // Wait for the ack response
    EXPECT_EQ(clients.back().receive_message()->get_message_type_id(), message_type_id::COMMON_ACK);

    // 3) Send log entries

    auto path99 = ds->get_blob_file(99).path();
    boost::filesystem::create_directories(path99.parent_path());
    std::ofstream ofs(path99.string());
    ofs << "Dummy data for blob 456";
    ofs.close();

    message_log_entries msg{100};
    msg.set_session_begin_flag(true);

    msg.add_normal_entry(1, "key1", "value1", {100, 1});
    msg.add_normal_with_blob(2, "key2", "value2", {200, 2},{99});
    msg.add_remove_entry(3, "key3", {300, 3});
    msg.add_clear_storage(4, {400, 4});
    msg.add_add_storage(5, {500, 5});
    msg.add_remove_storage(6, {600, 6});
    
    msg.set_flush_flag(true);
    msg.set_session_end_flag(true);


    ds->ready();
    clients.back().send_message(msg);
    // Wait for the ack response
    EXPECT_EQ(clients.back().receive_message()->get_message_type_id(), message_type_id::COMMON_ACK);

    // 4) Close the control session
    for (auto& client : clients) {
        client.close_session();
    }
    // shutdown the server
    server.shutdown();
    server_thread.join();

    // Check pwal log entries
    auto log_entries = read_log_file(std::string(replica) + "/pwal_0000");
    EXPECT_EQ(log_entries.size(), 6);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 100, 1, {}, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 200, 2, {99}, log_entry::entry_type::normal_with_blob));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 3, "key3", "", 300, 3, {}, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 4, "", "", 400, 4, {}, log_entry::entry_type::clear_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 5, "", "", 500, 5, {}, log_entry::entry_type::add_storage));
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 6, "", "", 600, 6, {}, log_entry::entry_type::remove_storage));
}

}  // namespace limestone::testing
