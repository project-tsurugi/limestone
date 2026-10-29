/*
 * Copyright 2026-2026 Project Tsurugi.
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
#include <gtest/gtest.h>

#include "replication/message_log_entries_wire_codec.h"
#include "replication/replication_message_io.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(message_log_entries_wire_codec_test, message_header_round_trip) {
    replication_message_io out("");
    message_log_entries_wire_codec::send_message_header(out, limestone::api::epoch_id_type{123}, 2U);

    replication_message_io in(out.get_out_string());
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

    replication_message_io out("");
    message_log_entries_wire_codec::send_entry_fixed_fields(out, entry);

    replication_message_io in(out.get_out_string());
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
    replication_message_io out("");
    message_log_entries_wire_codec::send_blob_count(out, 3U);

    replication_message_io in(out.get_out_string());
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

TEST(message_log_entries_wire_codec_test, decode_entry_type_returns_defined_values) {
    // a defined value is returned as is (the types not allowed in a message also pass here)
    for (auto const type : {log_entry::entry_type::this_id_is_not_used, log_entry::entry_type::normal_entry,
                            log_entry::entry_type::normal_with_blob, log_entry::entry_type::remove_entry,
                            log_entry::entry_type::marker_begin, log_entry::entry_type::marker_end,
                            log_entry::entry_type::marker_durable, log_entry::entry_type::marker_invalidated_begin,
                            log_entry::entry_type::clear_storage, log_entry::entry_type::add_storage,
                            log_entry::entry_type::remove_storage}) {
        EXPECT_EQ(message_log_entries_wire_codec::decode_entry_type(static_cast<std::uint8_t>(type)), type);
    }
}

TEST(message_log_entries_wire_codec_test, decode_entry_type_aborts_on_undefined_value) {
    // an undefined value aborts (discarding it silently would make the persistent data of the replica differ from
    // the master)
    EXPECT_DEATH((void) message_log_entries_wire_codec::decode_entry_type(0xFF), "undefined entry type value 255");
}

}  // namespace limestone::testing
