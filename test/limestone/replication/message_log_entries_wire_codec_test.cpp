#include <gtest/gtest.h>

#include "replication/message_log_entries_wire_codec.h"
#include "replication/socket_io.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(message_log_entries_wire_codec_test, message_header_round_trip) {
    socket_io out("");
    message_log_entries_wire_codec::send_message_header(out, limestone::api::epoch_id_type{123}, 2U);

    socket_io in(out.get_out_string());
    auto const header = message_log_entries_wire_codec::receive_message_header(in);

    EXPECT_EQ(header.epoch_id, limestone::api::epoch_id_type{123});
    EXPECT_EQ(header.entry_count, 2U);
}

TEST(message_log_entries_wire_codec_test, entry_fixed_fields_round_trip) {
    message_log_entries::entry entry{};
    entry.type = limestone::api::log_entry::entry_type::normal_entry;
    entry.storage_id = 456U;
    entry.key = "key";
    entry.value = "value";
    entry.write_version = limestone::api::write_version_type{789U, 10U};

    socket_io out("");
    message_log_entries_wire_codec::send_entry_fixed_fields(out, entry);

    socket_io in(out.get_out_string());
    auto const decoded = message_log_entries_wire_codec::receive_entry_fixed_fields(in);

    EXPECT_EQ(decoded.type, entry.type);
    EXPECT_EQ(decoded.storage_id, entry.storage_id);
    EXPECT_EQ(decoded.key, entry.key);
    EXPECT_EQ(decoded.value, entry.value);
    EXPECT_EQ(decoded.write_version.get_major(), entry.write_version.get_major());
    EXPECT_EQ(decoded.write_version.get_minor(), entry.write_version.get_minor());
    EXPECT_TRUE(decoded.blob_ids.empty());
}

TEST(message_log_entries_wire_codec_test, blob_count_round_trip) {
    socket_io out("");
    message_log_entries_wire_codec::send_blob_count(out, 3U);

    socket_io in(out.get_out_string());
    EXPECT_EQ(message_log_entries_wire_codec::receive_blob_count(in), 3U);
}

TEST(message_log_entries_wire_codec_test, apply_operation_flags_updates_message) {
    message_log_entries message{limestone::api::epoch_id_type{1}};
    auto const flags = static_cast<std::uint8_t>(
            message_log_entries::SESSION_BEGIN_FLAG | message_log_entries::FLUSH_FLAG);

    message_log_entries_wire_codec::apply_operation_flags(message, flags);

    EXPECT_TRUE(message.has_session_begin_flag());
    EXPECT_FALSE(message.has_session_end_flag());
    EXPECT_TRUE(message.has_flush_flag());
}

}  // namespace limestone::testing
