/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include "replication/rdma_log_channel_receiver.h"

#include "gtest/gtest.h"
#include "replication/message_log_entries.h"
#include "replication/replica_server.h"
#include "replication/replication_message_io.h"
#include "replication/tcp_replication_message_io.h"
#include "rdma/rdma_replication_message_io.h"
#include "rdma/rdma_send_stream_base.h"
#include <boost/filesystem.hpp>
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "replication_test_helper.h"
#include "test_rdma_frame_buffer.h"
#include "test_root.h"

namespace limestone::testing {

using namespace limestone::replication;

class rdma_log_channel_receiver_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/rdma_log_channel_receiver_test";

    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
    }

    void TearDown() override { boost::filesystem::remove_all(base_location); }
};

namespace {

// Epoch values used to verify message application. If the three epochs (the
// datastore's switched epoch, the message's session epoch, and the entries'
// write_version epoch) were all 0, a missed or mixed-up application would
// still pass as 0 == 0, so assign distinct non-zero values.
// The session epoch is verified through finished_epoch_id() because
// current_epoch_id() reverts to UINT64_MAX on end_session(), which is needed
// to flush the WAL.
constexpr epoch_id_type switched_epoch{5};  // passed to the datastore's switch_epoch(); must not leak into the session
constexpr epoch_id_type message_epoch{7};   // carried by the message; the session joins this epoch
constexpr epoch_id_type entry_epoch{3};     // carried by the entries' write_version

class testable_receiver : public rdma_log_channel_receiver {
public:
    using rdma_log_channel_receiver::rdma_log_channel_receiver;
    using rdma_log_channel_receiver::process_payload_locked;
};

struct receiver_context {
    std::unique_ptr<replica_server> server;
    std::unique_ptr<testable_receiver> receiver;
};

// Creates one log channel on the replica_server's datastore and returns a
// receiver bound to it.
receiver_context make_receiver_with_channel(std::string const& location) {
    receiver_context ctx{};
    ctx.server = std::make_unique<replica_server>();
    ctx.server->initialize(location);
    auto& ds = ctx.server->get_datastore();
    auto& channel = ds.create_channel();
    ctx.receiver = std::make_unique<testable_receiver>(ds, channel);
    return ctx;
}

rdma_data_event make_rdma_event_from_message(
    replication_message& message,
    std::uint16_t sequence_number) {
    replication_message_io out_io(std::string{});
    replication_message::send(out_io, message);
    std::string payload = out_io.get_out_string();

    rdma_data_event ev{};
    ev.header.version = rdma_frame_current_version;
    ev.header.flags = 0U;
    ev.header.sequence_number = sequence_number;
    ev.header.channel_id = 1U;
    ev.header.payload_size = static_cast<std::uint32_t>(payload.size());
    ev.payload.assign(payload.begin(), payload.end());
    return ev;
}

std::pair<rdma_data_event, rdma_data_event>
make_split_events(replication_message& message, std::uint16_t sequence_start) {
    replication_message_io out_io(std::string{});
    replication_message::send(out_io, message);
    std::string payload = out_io.get_out_string();
    std::size_t mid = payload.size() / 2;

    rdma_data_event first{};
    first.header.version = rdma_frame_current_version;
    first.header.flags = rdma_frame_flag_partial_payload;
    first.header.sequence_number = sequence_start;
    first.header.channel_id = 1U;
    first.header.payload_size = static_cast<std::uint32_t>(mid);
    first.payload.assign(payload.begin(), std::next(payload.begin(),
        static_cast<std::string::difference_type>(mid)));

    rdma_data_event second{};
    second.header.version = rdma_frame_current_version;
    second.header.flags = 0U;
    second.header.sequence_number = static_cast<std::uint16_t>(sequence_start + 1U);
    second.header.channel_id = 1U;
    second.header.payload_size = static_cast<std::uint32_t>(payload.size() - mid);
    second.payload.assign(std::next(payload.begin(),
        static_cast<std::string::difference_type>(mid)), payload.end());
    return {std::move(first), std::move(second)};
}

class capturing_rdma_send_stream : public rdma_send_stream_base {
public:
    [[nodiscard]] std::unique_ptr<rdma_frame_buffer_base> acquire_frame_buffer(
            std::size_t max_payload,
            std::size_t min_capacity) noexcept override {
        auto capacity = granted_frame_capacity(max_payload);
        if (max_frame_capacity_ != 0U) {
            capacity = std::min(capacity, max_frame_capacity_);
        }
        // A real stream retries until it can grant min_capacity, so it never hands back an
        // undersized frame. Honour that contract here too.
        capacity = std::max(capacity, min_capacity);
        return std::make_unique<test_rdma_frame_buffer>(capacity);
    }

    [[nodiscard]] send_result submit_frame_buffer(
            rdma_frame_buffer_base& frame,
            std::size_t             payload_size) override {
        auto& test_frame = dynamic_cast<test_rdma_frame_buffer&>(frame);
        calls_.emplace_back(test_frame.take_written(payload_size));
        return {true, "", payload_size};
    }

    [[nodiscard]] flush_result flush(std::chrono::milliseconds) noexcept override {
        return {true, ""};
    }

    /**
     * @brief Upper bound on the capacity granted by acquire_frame_buffer().
     *
     * A value of 0 grants the full requested payload. A non-zero value caps the grant,
     * forcing callers to split their payload across several frames as a real send ring
     * would.
     */
    std::size_t max_frame_capacity_{};
    std::vector<std::vector<std::uint8_t>> calls_{};
};

rdma_data_event make_rdma_event_from_payload(
        std::vector<std::uint8_t> const& payload,
        std::uint16_t sequence_number) {
    rdma_data_event ev{};
    ev.header.version = rdma_frame_current_version;
    ev.header.flags = 0U;
    ev.header.sequence_number = sequence_number;
    ev.header.channel_id = 1U;
    ev.header.payload_size = static_cast<std::uint32_t>(payload.size());
    ev.payload = payload;
    return ev;
}

}  // namespace

TEST_F(rdma_log_channel_receiver_test, handle_rdma_data_event_applies_entries) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);
    ctx.server->get_datastore().switch_epoch(switched_epoch);

    message_log_entries entries(message_epoch);
    entries.set_session_begin_flag(true);
    entries.set_session_end_flag(true);
    entries.add_normal_entry(1U, "k", "v", write_version_type{entry_epoch, 0U});
    auto ev = make_rdma_event_from_message(entries, 0U);

    ctx.receiver->handle_rdma_data_event(ev);

    auto& channel = ctx.receiver->get_log_channel();
    EXPECT_EQ(channel.finished_epoch_id(), message_epoch);
    auto replica_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(replica_entries.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1U, "k", "v", entry_epoch, 0U, {},
                               log_entry::entry_type::normal_entry));
}

TEST_F(rdma_log_channel_receiver_test, handle_rdma_data_event_payload_size_mismatch_fatals) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    ctx.receiver->get_log_channel().begin_session();

    message_log_entries entries(epoch_id_type{0});
    entries.add_normal_entry(1U, "k", "v", write_version_type{epoch_id_type{0}, 0U});
    auto ev = make_rdma_event_from_message(entries, 0U);
    ev.header.payload_size += 1U;  // mismatch

    EXPECT_DEATH({ ctx.receiver->handle_rdma_data_event(ev); },
                 "RDMA payload size mismatch");
}

TEST_F(rdma_log_channel_receiver_test, handle_rdma_data_event_partial_then_complete_applies_once) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);
    ctx.server->get_datastore().switch_epoch(switched_epoch);

    message_log_entries entries(message_epoch);
    entries.set_session_begin_flag(true);
    entries.set_session_end_flag(true);
    entries.add_normal_entry(1U, "k", "v", write_version_type{entry_epoch, 0U});
    auto events = make_split_events(entries, 0U);

    auto& channel = ctx.receiver->get_log_channel();

    ctx.receiver->handle_rdma_data_event(events.first);
    // Nothing is applied to the WAL while only the partial frame has arrived.
    auto replica_pwal_path = boost::filesystem::path(base_location) / "pwal_0000";
    if (boost::filesystem::exists(replica_pwal_path)) {
        EXPECT_TRUE(read_log_file(base_location, "pwal_0000").empty());
    }

    ctx.receiver->handle_rdma_data_event(events.second);

    EXPECT_EQ(channel.finished_epoch_id(), message_epoch);
    // Verify through the WAL entry count that the message was applied exactly
    // once (0 would mean not applied, 2 would mean applied twice).
    auto replica_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(replica_entries.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1U, "k", "v", entry_epoch, 0U, {},
                               log_entry::entry_type::normal_entry));
}

TEST_F(rdma_log_channel_receiver_test, handle_rdma_data_event_version_mismatch_fatals) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    message_log_entries entries(epoch_id_type{0});
    entries.set_session_begin_flag(true);
    auto events = make_split_events(entries, 0U);

    ctx.receiver->handle_rdma_data_event(events.first);

    auto bad = events.second;
    bad.header.version = static_cast<std::uint8_t>(events.second.header.version + 1U);
    EXPECT_DEATH({ ctx.receiver->handle_rdma_data_event(bad); },
                 "RDMA frame version mismatch");
}

TEST_F(rdma_log_channel_receiver_test, process_payload_locked_processes_single_message) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);
    ctx.server->get_datastore().switch_epoch(switched_epoch);

    message_log_entries entries(message_epoch);
    entries.set_session_begin_flag(true);
    entries.set_session_end_flag(true);
    entries.add_normal_entry(1U, "k", "v", write_version_type{entry_epoch, 0U});

    replication_message_io out_io(std::string{});
    replication_message::send(out_io, entries);
    std::string payload = out_io.get_out_string();
    std::vector<std::uint8_t> aggregated(payload.begin(), payload.end());

    rdma_frame_header header{};
    header.version = rdma_frame_current_version;
    header.flags = 0U;
    header.sequence_number = 7U;
    header.channel_id = 1U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.receiver->process_payload_locked(aggregated, header);

    auto& channel = ctx.receiver->get_log_channel();
    EXPECT_EQ(channel.finished_epoch_id(), message_epoch);
    auto replica_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(replica_entries.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1U, "k", "v", entry_epoch, 0U, {},
                               log_entry::entry_type::normal_entry));
}

TEST_F(rdma_log_channel_receiver_test,
       handle_rdma_data_event_processes_sequential_messages) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    ctx.server->get_datastore().switch_epoch(switched_epoch);

    message_log_entries first(message_epoch);
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{entry_epoch, 0U});
    message_log_entries second(message_epoch);
    second.set_session_end_flag(true);
    second.add_normal_entry(2U, "k2", "v2", write_version_type{entry_epoch, 0U});

    auto ev1 = make_rdma_event_from_message(first, 0U);
    auto ev2 = make_rdma_event_from_message(second, 1U);

    ctx.receiver->handle_rdma_data_event(ev1);
    ctx.receiver->handle_rdma_data_event(ev2);

    auto& channel = ctx.receiver->get_log_channel();
    EXPECT_EQ(channel.finished_epoch_id(), message_epoch);
    auto replica_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(replica_entries.size(), 2U);
    EXPECT_TRUE(AssertLogEntry(replica_entries[0], 1U, "k1", "v1", entry_epoch, 0U, {},
                               log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(replica_entries[1], 2U, "k2", "v2", entry_epoch, 0U, {},
                               log_entry::entry_type::normal_entry));
}

// Verify that process_payload_locked processes all messages packed in one payload.
// This exercises the batched-send path where multiple message_log_entries are accumulated
// in rdma_serializer_io_ and sent as a single RDMA write.
TEST_F(rdma_log_channel_receiver_test,
       process_payload_locked_processes_multiple_messages_in_single_payload) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    // Build two messages and concatenate them into one payload (simulating RDMA batch flush).
    // The first message carries session_begin_flag so that the channel opens the WAL file.
    message_log_entries first(epoch_id_type{1});
    first.set_session_begin_flag(true);
    first.add_normal_entry(1U, "k1", "v1", write_version_type{epoch_id_type{1}, 0U});
    message_log_entries second(epoch_id_type{1});
    second.add_normal_entry(2U, "k2", "v2", write_version_type{epoch_id_type{1}, 0U});
    second.set_session_end_flag(true);

    replication_message_io out(std::string{});
    replication_message::send(out, first);
    replication_message::send(out, second);
    std::string combined = out.get_out_string();
    std::vector<std::uint8_t> aggregated(combined.begin(), combined.end());

    rdma_frame_header header{};
    header.version = rdma_frame_current_version;
    header.flags = 0U;
    header.sequence_number = 0U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.receiver->process_payload_locked(aggregated, header);

    // Verify both entries were written to the WAL file.
    auto log_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(log_entries.size(), 2U);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1U, "k1", "v1", 1U, 0U, {},
                               log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2U, "k2", "v2", 1U, 0U, {},
                               log_entry::entry_type::normal_entry));
}

TEST_F(rdma_log_channel_receiver_test,
       process_payload_locked_receiver_exception_fatals) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    std::vector<std::uint8_t> invalid_payload{
        static_cast<std::uint8_t>(message_type_id::COMMON_ACK)};

    rdma_frame_header header{};
    header.version = rdma_frame_current_version;
    header.flags = 0U;
    header.sequence_number = 0U;
    header.payload_size = static_cast<std::uint32_t>(invalid_payload.size());

    EXPECT_DEATH({ ctx.receiver->process_payload_locked(invalid_payload, header); },
                 "RDMA receiver failed while processing payload");
}

// Verify that a BLOB entry received over RDMA is correctly deserialised and
// written as a blob file in the replica datastore.
TEST_F(rdma_log_channel_receiver_test, process_payload_locked_with_blob_entry_writes_blob_file) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    // Build a sender-side datastore in a separate directory to create the blob file.
    constexpr const char* sender_dir = "/tmp/rdma_log_channel_receiver_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    blob_id_type blob_id = 55U;
    std::string const blob_content = "receiver_rdma_blob_test_data";
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs.write(
            blob_content.data(),
            static_cast<std::streamsize>(blob_content.size()));
    }

    // Serialise the message including the blob data using tcp_replication_message_io (string mode).
    replication::tcp_replication_message_io sender_io(std::string{}, *sender_ds);
    message_log_entries entries(epoch_id_type{5});
    entries.set_session_begin_flag(true);
    entries.add_normal_with_blob(1U, "bk", "bv",
        write_version_type{epoch_id_type{5}, 0U}, {blob_id});
    replication_message::send(sender_io, entries);
    std::string payload_str = sender_io.get_out_string();

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);

    // Build an RDMA aggregated payload and pass it to the receiver.
    std::vector<std::uint8_t> aggregated(payload_str.begin(), payload_str.end());
    rdma_frame_header header{};
    header.version = rdma_frame_current_version;
    header.flags = 0U;
    header.sequence_number = 0U;
    header.payload_size = static_cast<std::uint32_t>(aggregated.size());

    ctx.receiver->process_payload_locked(aggregated, header);

    // Verify the blob file was written to the server's replica datastore.
    auto& server_ds = ctx.server->get_datastore();
    auto received_path = server_ds.get_blob_file(blob_id).path();
    ASSERT_TRUE(boost::filesystem::exists(received_path))
        << "Blob file not created at: " << received_path;
    std::ifstream ifs(received_path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), blob_content);
}

TEST_F(rdma_log_channel_receiver_test,
       handle_rdma_data_event_accepts_blob_split_by_rdma_replication_message_io) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    constexpr const char* sender_dir = "/tmp/rdma_log_channel_receiver_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    blob_id_type blob_id = 56U;
    std::string const blob_content = "rdma_replication_message_io_split_blob_payload";
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    capturing_rdma_send_stream stream{};
    rdma_replication_message_io sender_io(stream, *sender_ds);
    message_log_entries entries(epoch_id_type{6});
    entries.set_session_begin_flag(true);
    entries.add_normal_with_blob(1U, "bk", "bv",
        write_version_type{epoch_id_type{6}, 0U}, {blob_id});

    replication_message::send(sender_io, entries);
    auto remaining = sender_io.get_out_string();
    if (! remaining.empty()) {
        ASSERT_TRUE(stream.send_all_bytes(remaining).success);
    }

    ASSERT_GE(stream.calls_.size(), 2U)
        << "rdma_replication_message_io should split a BLOB message into multiple RDMA sends";

    for (std::size_t i = 0; i < stream.calls_.size(); ++i) {
        auto event = make_rdma_event_from_payload(
            stream.calls_[i], static_cast<std::uint16_t>(i));
        EXPECT_NO_THROW({ ctx.receiver->handle_rdma_data_event(event); });
    }

    auto& server_ds = ctx.server->get_datastore();
    auto received_path = server_ds.get_blob_file(blob_id).path();
    ASSERT_TRUE(boost::filesystem::exists(received_path))
        << "Blob file not created at: " << received_path;
    std::ifstream ifs(received_path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), blob_content);

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);
}

TEST_F(rdma_log_channel_receiver_test,
       handle_rdma_data_event_with_partial_blob_does_not_write_pwal_until_complete) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    constexpr const char* sender_dir = "/tmp/rdma_log_channel_receiver_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    constexpr blob_id_type blob_id = 58U;
    std::string const blob_content(4096, 'b');
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs << blob_content;
    }

    capturing_rdma_send_stream stream{};
    stream.max_frame_capacity_ = 256U;
    rdma_replication_message_io sender_io(stream, *sender_ds);
    message_log_entries entries(epoch_id_type{7});
    entries.set_session_begin_flag(true);
    entries.set_session_end_flag(true);
    entries.add_normal_with_blob(1U, "bk", "bv",
        write_version_type{epoch_id_type{7}, 0U}, {blob_id});

    replication_message::send(sender_io, entries);
    auto remaining = sender_io.get_out_string();
    if (! remaining.empty()) {
        ASSERT_TRUE(stream.send_all_bytes(remaining).success);
    }

    ASSERT_GE(stream.calls_.size(), 2U);

    ctx.receiver->handle_rdma_data_event(
        make_rdma_event_from_payload(stream.calls_.front(), 0U));

    auto replica_pwal_path = boost::filesystem::path(base_location) / "pwal_0000";
    if (boost::filesystem::exists(replica_pwal_path)) {
        auto replica_entries = read_log_file(base_location, "pwal_0000");
        EXPECT_TRUE(replica_entries.empty())
            << "PWAL must stay empty until the split BLOB payload is fully received";
    }

    for (std::size_t i = 1; i < stream.calls_.size(); ++i) {
        ctx.receiver->handle_rdma_data_event(
            make_rdma_event_from_payload(
                stream.calls_[i], static_cast<std::uint16_t>(i)));
    }

    auto replica_entries = read_log_file(base_location, "pwal_0000");
    ASSERT_EQ(replica_entries.size(), 1U);
    EXPECT_TRUE(AssertLogEntry(
        replica_entries[0], 1U, "bk", "bv", 7U, 0U, {blob_id},
        log_entry::entry_type::normal_with_blob));

    auto received_path = ctx.server->get_datastore().get_blob_file(blob_id).path();
    ASSERT_TRUE(boost::filesystem::exists(received_path));
    std::ifstream ifs(received_path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), blob_content);

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);
}

TEST_F(rdma_log_channel_receiver_test,
       handle_rdma_data_event_with_partial_blob_version_mismatch_fatals) {
    auto ctx = make_receiver_with_channel(base_location);
    ASSERT_NE(ctx.receiver, nullptr);

    constexpr const char* sender_dir = "/tmp/rdma_log_channel_receiver_test_sender";
    boost::filesystem::remove_all(sender_dir);
    boost::filesystem::create_directories(sender_dir);
    limestone::api::configuration sender_conf{};
    sender_conf.set_data_location(sender_dir);
    auto sender_ds = std::make_unique<limestone::api::datastore_test>(sender_conf);

    constexpr blob_id_type blob_id = 59U;
    std::string const blob_content(4096, 'c');
    auto blob_path = sender_ds->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(blob_path.parent_path());
    {
        std::ofstream ofs(blob_path.string(), std::ios::binary);
        ofs.write(
            blob_content.data(),
            static_cast<std::streamsize>(blob_content.size()));
    }

    capturing_rdma_send_stream stream{};
    stream.max_frame_capacity_ = 256U;
    rdma_replication_message_io sender_io(stream, *sender_ds);
    message_log_entries entries(epoch_id_type{8});
    entries.set_session_begin_flag(true);
    entries.set_session_end_flag(true);
    entries.add_normal_with_blob(1U, "abort-key", "abort-value",
        write_version_type{epoch_id_type{8}, 0U}, {blob_id});

    replication_message::send(sender_io, entries);
    auto remaining = sender_io.get_out_string();
    if (! remaining.empty()) {
        ASSERT_TRUE(stream.send_all_bytes(remaining).success);
    }
    ASSERT_GE(stream.calls_.size(), 2U);

    auto partial_blob_path = ctx.server->get_datastore().get_blob_file(blob_id).path();
    std::size_t next_call = 0;
    while (next_call + 1 < stream.calls_.size()
            && !boost::filesystem::exists(partial_blob_path)) {
        ctx.receiver->handle_rdma_data_event(
            make_rdma_event_from_payload(
                stream.calls_[next_call],
                static_cast<std::uint16_t>(next_call)));
        ++next_call;
    }
    EXPECT_TRUE(boost::filesystem::exists(partial_blob_path));

    // A bad frame after the partial BLOB body is now fatal because the RDMA
    // stream can no longer be trusted once frame validation fails.
    ASSERT_LT(next_call, stream.calls_.size());
    auto bad_event = make_rdma_event_from_payload(
        stream.calls_[next_call], static_cast<std::uint16_t>(next_call));
    bad_event.header.version = static_cast<std::uint8_t>(rdma_frame_current_version + 1U);
    EXPECT_DEATH({ ctx.receiver->handle_rdma_data_event(bad_event); },
                 "RDMA frame version mismatch");

    sender_ds.reset();
    boost::filesystem::remove_all(sender_dir);
}

}  // namespace limestone::testing
