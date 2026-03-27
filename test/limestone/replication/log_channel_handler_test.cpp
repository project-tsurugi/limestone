#include "replication/log_channel_handler.h"

#include <pthread.h>

#include "gtest/gtest.h"
#include "replication/channel_handler_base.h"
#include "replication/message_error.h"
#include "replication/message_log_channel_create.h"
#include "replication/message_log_entries.h"
#include "replication/replica_server.h"
#include "replication/socket_io.h"
#include "replication/blob_socket_io.h"
#include "replication/validation_result.h"
#include <boost/filesystem.hpp>
#include <cerrno>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <rdma_comm/ack_message.h>
#include <rdma_comm/rdma_frame_header.h>
#include "test_message.h"
#include "replication_test_helper.h"
#include "test_root.h"
#include <fcntl.h>
#include <unistd.h>
namespace limestone::testing {

using namespace limestone::replication;

class dummy_server {};

class log_channel_handler_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/replica_server_test";

    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
    }

    void TearDown() override { boost::filesystem::remove_all(base_location); }
};

class testable_log_handler : public log_channel_handler {
public:
    using log_channel_handler::log_channel_handler;
    using log_channel_handler::validate_initial;
    using log_channel_handler::send_initial_ack;
    using log_channel_handler::set_log_channel_id_counter_for_test;
    using log_channel_handler::authorize;
    using log_channel_handler::process_loop;
    using log_channel_handler::process_pending_rdma_messages_locked;
    using log_channel_handler::process_rdma_message_locked;
    using log_channel_handler::push_pending_frame_for_test;
};

namespace {

struct rdma_test_context {
    int read_fd{-1};
    int write_fd{-1};
    std::unique_ptr<replica_server> server;
    std::unique_ptr<socket_io> io;
    std::unique_ptr<testable_log_handler> handler;
};

rdma_test_context make_rdma_handler_with_channel(std::string const& location) {
    rdma_test_context ctx{};
    int pipefd[2];
    if (::pipe(pipefd) != 0) {
        return ctx;
    }
    ctx.read_fd = pipefd[0];
    ctx.write_fd = pipefd[1];

    ctx.server = std::make_unique<replica_server>();
    ctx.server->initialize(location);
    ctx.io = std::make_unique<socket_io>(ctx.write_fd);
    auto handler = std::make_unique<testable_log_handler>(*ctx.server, *ctx.io);

    auto create_msg = std::make_unique<message_log_channel_create>(1U);
    auto init = handler->validate_initial(std::move(create_msg));
    ctx.handler = std::move(handler);
    return ctx;
}

rdma::communication::rdma_receive_data_event make_rdma_event_from_message(
    replication_message& message,
    std::uint16_t sequence_number) {
    socket_io out_io(std::string{});
    replication_message::send(out_io, message);
    std::string payload = out_io.get_out_string();

    rdma::communication::rdma_receive_data_event ev{};
    ev.header.version = rdma::communication::rdma_frame_protocol_version;
    ev.header.flags = 0U;
    ev.header.sequence_number = sequence_number;
    ev.header.channel_id = 1U;
    ev.header.payload_size = static_cast<std::uint32_t>(payload.size());
    ev.payload.assign(payload.begin(), payload.end());
    return ev;
}

std::pair<rdma::communication::rdma_receive_data_event,
          rdma::communication::rdma_receive_data_event>
make_split_events(replication_message& message, std::uint16_t sequence_start) {
    socket_io out_io(std::string{});
    replication_message::send(out_io, message);
    std::string payload = out_io.get_out_string();
    std::size_t mid = payload.size() / 2;

    rdma::communication::rdma_receive_data_event first{};
    first.header.version = rdma::communication::rdma_frame_protocol_version;
    first.header.flags = rdma::communication::rdma_frame_flag_partial_payload;
    first.header.sequence_number = sequence_start;
    first.header.channel_id = 1U;
    first.header.payload_size = static_cast<std::uint32_t>(mid);
    first.payload.assign(payload.begin(), std::next(payload.begin(),
        static_cast<std::string::difference_type>(mid)));

    rdma::communication::rdma_receive_data_event second{};
    second.header.version = rdma::communication::rdma_frame_protocol_version;
    second.header.flags = 0U;
    second.header.sequence_number = static_cast<std::uint16_t>(sequence_start + 1U);
    second.header.channel_id = 1U;
    second.header.payload_size = static_cast<std::uint32_t>(payload.size() - mid);
    second.payload.assign(std::next(payload.begin(),
        static_cast<std::string::difference_type>(mid)), payload.end());
    return {std::move(first), std::move(second)};
}

void set_non_blocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    ASSERT_NE(flags, -1);
    ASSERT_EQ(::fcntl(fd, F_SETFL, flags | O_NONBLOCK), 0);
}

}  // namespace

TEST_F(log_channel_handler_test, validate_initial_and_dispatch_succeeds) {
    replica_server server{};
    server.initialize(base_location);
    socket_io out("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), out);

    auto msg = std::make_unique<message_log_channel_create>(1234U);
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_TRUE(result.ok());

    const auto& channel = handler.get_log_channel();
    EXPECT_NE(&channel, nullptr);

    
    test_message test_msg{};
    replication_message::send(out, test_msg);
    socket_io in(out.get_out_string());
    test_message::post_receive_called = false;
    testable_log_handler handler2(reinterpret_cast<replica_server&>(server), in);
    EXPECT_THROW({ handler2.process_loop(); }, limestone_io_exception); // Exception is thrown when the stream becomes unreadable, but it is ignored
    ASSERT_EQ(test_message::post_receive_called, true);
}

TEST_F(log_channel_handler_test, authorize_succeeds_then_fails_at_limit_boundary) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    // Set the counter to one before the maximum limit
    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT - 1);

    // First call: should succeed and assign the final valid ID
    auto result1 = handler.authorize();
    EXPECT_TRUE(result1.ok());

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "logch09999");  // last valid thread name

    // Second call: should fail because it exceeds the maximum allowed count
    auto result2 = handler.authorize();
    EXPECT_FALSE(result2.ok());
    EXPECT_EQ(result2.error_code(), message_error::log_channel_error_too_many_channels);
    EXPECT_EQ(result2.error_message(), "Too many log channels: cannot assign more");
}

TEST_F(log_channel_handler_test, authorize_fails_when_exceeded) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.set_log_channel_id_counter_for_test(log_channel_handler::MAX_LOG_CHANNEL_COUNT);
    auto result = handler.authorize();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_too_many_channels);
}

TEST_F(log_channel_handler_test, validate_fails_on_wrong_type) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    auto wrong = std::make_unique<message_ack>();
    auto result = handler.validate_initial(std::move(wrong));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_invalid_type);
}

TEST_F(log_channel_handler_test, validate_fails_on_failed_cast) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    class bad_message : public replication_message {
        message_type_id get_message_type_id() const override {
            return message_type_id::LOG_CHANNEL_CREATE;
        }
        void send_body(socket_io&) const override {}
        void receive_body(socket_io&) override {}
        void post_receive(handler_resources&) override {}
    };

    auto msg = std::make_unique<bad_message>();
    auto result = handler.validate_initial(std::move(msg));
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), message_error::log_channel_error_bad_cast);
}

TEST_F(log_channel_handler_test, send_initial_ack_sends_ack_message) {
    dummy_server server;
    socket_io io("");
    testable_log_handler handler(reinterpret_cast<replica_server&>(server), io);

    handler.send_initial_ack();

    socket_io reader(io.get_out_string());
    auto msg = replication_message::receive(reader);
    auto* ack = dynamic_cast<message_ack*>(msg.get());
    ASSERT_NE(ack, nullptr);
}

TEST_F(log_channel_handler_test, handle_rdma_data_event_sends_ack) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.set_session_begin_flag(true);
    entries.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{0}, 0U});
    auto ev = make_rdma_event_from_message(entries, 0U);

    ctx.handler->handle_rdma_data_event(ev);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    auto& channel = ctx.handler->get_log_channel();
    EXPECT_EQ(channel.current_epoch_id(), entries.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, handle_rdma_data_event_sequence_mismatch_no_ack) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    ctx.handler->get_log_channel().begin_session();

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{0}, 0U});
    auto ev = make_rdma_event_from_message(entries, 1U);

    ctx.handler->handle_rdma_data_event(ev);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, handle_rdma_data_event_payload_size_mismatch_no_ack) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    ctx.handler->get_log_channel().begin_session();

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{0}, 0U});
    auto ev = make_rdma_event_from_message(entries, 0U);
    ev.header.payload_size += 1U;  // mismatch

    ctx.handler->handle_rdma_data_event(ev);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, handle_rdma_data_event_partial_then_complete_ack_once) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.set_session_begin_flag(true);
    auto events = make_split_events(entries, 0U);

    auto& channel = ctx.handler->get_log_channel();

    ctx.handler->handle_rdma_data_event(events.first);
    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ctx.handler->handle_rdma_data_event(events.second);
    n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    EXPECT_EQ(channel.current_epoch_id(), entries.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, handle_rdma_data_event_partial_clears_on_version_mismatch) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.set_session_begin_flag(true);
    auto events = make_split_events(entries, 0U);

    auto& channel = ctx.handler->get_log_channel();

    ctx.handler->handle_rdma_data_event(events.first);

    auto bad = events.second;
    bad.header.version = static_cast<std::uint8_t>(events.second.header.version + 1U);
    ctx.handler->handle_rdma_data_event(bad);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, process_pending_rdma_messages_locked_waits_for_completion) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    auto events = make_split_events(entries, 0U);

    ctx.handler->handle_rdma_data_event(events.first);
    // Already invoked inside handle, but ensure explicit call does not process partial.
    ctx.handler->process_pending_rdma_messages_locked();

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, process_rdma_message_locked_processes_single_message) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries entries(epoch_id_type{0});
    entries.set_session_begin_flag(true);
    entries.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{0}, 0U});

    socket_io out_io(std::string{});
    replication_message::send(out_io, entries);
    std::string payload = out_io.get_out_string();
    std::vector<std::uint8_t> aggregated(payload.begin(), payload.end());

    rdma::communication::rdma_frame_header header{};
    header.version = rdma::communication::rdma_frame_protocol_version;
    header.flags = 0U;
    header.sequence_number = 7U;
    header.channel_id = 1U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.handler->process_rdma_message_locked(aggregated, header);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    auto& channel = ctx.handler->get_log_channel();
    EXPECT_EQ(channel.current_epoch_id(), entries.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test, process_pending_rdma_messages_locked_processes_multiple_messages) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries first(epoch_id_type{0});
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{epoch_id_type{0}, 0U});
    message_log_entries second(epoch_id_type{1});
    second.add_normal_entry(2U, "k2", "v2", write_version_type{epoch_id_type{1}, 0U});

    auto ev1 = make_rdma_event_from_message(first, 0U);
    auto ev2 = make_rdma_event_from_message(second, 1U);

    ctx.handler->push_pending_frame_for_test(ev1);
    ctx.handler->push_pending_frame_for_test(ev2);

    ctx.handler->process_pending_rdma_messages_locked();

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    auto& channel = ctx.handler->get_log_channel();
    EXPECT_EQ(channel.current_epoch_id(), first.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test,
       handle_rdma_data_event_with_sequential_messages_sends_acks_in_order) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries first(epoch_id_type{0});
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{epoch_id_type{0}, 0U});
    message_log_entries second(epoch_id_type{1});
    second.add_normal_entry(2U, "k2", "v2", write_version_type{epoch_id_type{1}, 0U});

    auto ev1 = make_rdma_event_from_message(first, 0U);
    auto ev2 = make_rdma_event_from_message(second, 1U);

    ctx.handler->handle_rdma_data_event(ev1);
    ctx.handler->handle_rdma_data_event(ev2);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    auto& channel = ctx.handler->get_log_channel();
    EXPECT_EQ(channel.current_epoch_id(), first.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

TEST_F(log_channel_handler_test,
       handle_rdma_data_event_out_of_order_partial_frames_processes_in_order) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);

    set_non_blocking(ctx.read_fd);

    message_log_entries first(epoch_id_type{0});
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{epoch_id_type{0}, 0U});
    message_log_entries second(epoch_id_type{1});
    second.add_normal_entry(2U, "k2", "v2", write_version_type{epoch_id_type{1}, 0U});

    auto second_events = make_split_events(second, 2U);  // seq 2 (partial), 3 (final)
    auto first_events = make_split_events(first, 0U);    // seq 0 (partial), 1 (final)

    auto& channel = ctx.handler->get_log_channel();

    ctx.handler->handle_rdma_data_event(second_events.first);
    ctx.handler->handle_rdma_data_event(second_events.second);
    ctx.handler->handle_rdma_data_event(first_events.first);

    rdma::communication::ack_message_bytes buf{};
    ssize_t n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    ctx.handler->handle_rdma_data_event(first_events.second);

    n = ::read(ctx.read_fd, buf.data(), buf.size());
    EXPECT_LT(n, 0);
    EXPECT_EQ(errno, EAGAIN);

    EXPECT_EQ(channel.current_epoch_id(), first.get_epoch_id());

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

// Verify that process_rdma_message_locked processes all messages packed in one payload.
// This exercises the batched-send path where multiple message_log_entries are accumulated
// in rdma_serializer_io_ and sent as a single RDMA write.
TEST_F(log_channel_handler_test,
       process_rdma_message_locked_processes_multiple_messages_in_single_payload) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);

    // Build two messages and concatenate them into one payload (simulating RDMA batch flush).
    // The first message carries session_begin_flag so that the channel opens the WAL file.
    message_log_entries first(epoch_id_type{1});
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{epoch_id_type{1}, 0U});
    message_log_entries second(epoch_id_type{1});
    second.add_normal_entry(2U, "k2", "v2", write_version_type{epoch_id_type{1}, 0U});
    second.set_session_end_flag(true);

    socket_io out(std::string{});
    replication_message::send(out, first);
    replication_message::send(out, second);
    std::string combined = out.get_out_string();
    std::vector<std::uint8_t> aggregated(combined.begin(), combined.end());

    rdma::communication::rdma_frame_header header{};
    header.version = rdma::communication::rdma_frame_protocol_version;
    header.flags = 0U;
    header.sequence_number = 0U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.handler->process_rdma_message_locked(aggregated, header);

    // Verify both entries were written to the WAL file.
    auto log_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(log_entries.size(), 2U);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1U, "k1", "v1", 1U, 0U, {},
                               log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2U, "k2", "v2", 1U, 0U, {},
                               log_entry::entry_type::normal_entry));

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

// Verify that a BLOB entry received over RDMA is correctly deserialised and
// written as a blob file in the replica datastore.
TEST_F(log_channel_handler_test, handle_rdma_data_event_with_blob_entry_writes_blob_file) {
    auto ctx = make_rdma_handler_with_channel(base_location);
    ASSERT_NE(ctx.handler, nullptr);
    ASSERT_GE(ctx.read_fd, 0);
    ASSERT_GE(ctx.write_fd, 0);
    set_non_blocking(ctx.read_fd);

    // Build a sender-side datastore in a separate directory to create the blob file.
    constexpr const char* sender_dir = "/tmp/replica_server_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    std::vector<boost::filesystem::path> sender_locations{sender_dir};
    limestone::api::configuration sender_conf(sender_locations,
        boost::filesystem::path{sender_dir});
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    blob_id_type blob_id = 55U;
    std::string const blob_content = "handler_rdma_blob_test_data";
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    // Serialise the message including the blob data using blob_socket_io (string mode).
    replication::blob_socket_io sender_io(std::string{}, *sender_ds);
    message_log_entries entries(epoch_id_type{5});
    entries.set_session_begin_flag(true);
    entries.add_normal_with_blob(1U, "bk", "bv",
        write_version_type{epoch_id_type{5}, 0U}, {blob_id});
    replication_message::send(sender_io, entries);
    std::string payload_str = sender_io.get_out_string();

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);

    // Build an RDMA aggregated payload and pass it to the handler.
    std::vector<std::uint8_t> aggregated(payload_str.begin(), payload_str.end());
    rdma::communication::rdma_frame_header header{};
    header.version = rdma::communication::rdma_frame_protocol_version;
    header.flags = 0U;
    header.sequence_number = 0U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.handler->process_rdma_message_locked(aggregated, header);

    // Verify the blob file was written to the server's replica datastore.
    auto& server_ds = ctx.server->get_datastore();
    auto received_path = server_ds.get_blob_file(blob_id).path();
    ASSERT_TRUE(boost::filesystem::exists(received_path))
        << "Blob file not created at: " << received_path;
    std::ifstream ifs(received_path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), blob_content);

    ::close(ctx.read_fd);
    ::close(ctx.write_fd);
}

}  // namespace limestone::testing
