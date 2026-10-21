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

#include <cstdint>
#include <string_view>

#include "test_root.h"

#include "log_entry/log_entry_format.h"

namespace limestone::testing {

using namespace std::literals;
namespace format = limestone::internal::log_entry_format;

// the sizes of each entry type match the format table
TEST(log_entry_format_test, fixed_sizes) {
    EXPECT_EQ(format::write_version_size, 16U);
    EXPECT_EQ(format::normal_entry_fixed_size, 33U);
    EXPECT_EQ(format::normal_with_blob_fixed_size, 37U);
    EXPECT_EQ(format::remove_entry_fixed_size, 29U);
    EXPECT_EQ(format::storage_entry_size, 25U);
    EXPECT_EQ(format::marker_size, 9U);
    EXPECT_EQ(format::marker_end_size, 10U);
}

// the classification functions classify each data, storage and marker type correctly
TEST(log_entry_format_test, classification) {
    using format::entry_type;
    for (auto t : {entry_type::normal_entry, entry_type::normal_with_blob, entry_type::remove_entry}) {
        EXPECT_TRUE(format::is_data_entry(t));
        EXPECT_FALSE(format::is_storage_entry(t));
        EXPECT_FALSE(format::is_marker(t));
    }
    for (auto t : {entry_type::clear_storage, entry_type::add_storage, entry_type::remove_storage}) {
        EXPECT_FALSE(format::is_data_entry(t));
        EXPECT_TRUE(format::is_storage_entry(t));
        EXPECT_FALSE(format::is_marker(t));
    }
    for (auto t : {entry_type::marker_begin, entry_type::marker_end, entry_type::marker_durable,
                   entry_type::marker_invalidated_begin}) {
        EXPECT_FALSE(format::is_data_entry(t));
        EXPECT_FALSE(format::is_storage_entry(t));
        EXPECT_TRUE(format::is_marker(t));
    }
    EXPECT_FALSE(format::is_data_entry(entry_type::this_id_is_not_used));
    EXPECT_FALSE(format::is_storage_entry(entry_type::this_id_is_not_used));
    EXPECT_FALSE(format::is_marker(entry_type::this_id_is_not_used));
}

// only 1 to 10 are valid entry type values
TEST(log_entry_format_test, is_valid_entry_type) {
    for (int value = 0; value <= 0xff; ++value) {
        EXPECT_EQ(format::is_valid_entry_type(static_cast<std::uint8_t>(value)), 1 <= value && value <= 10) << value;
    }
}

// equal key_sids compare as 0
TEST(log_entry_format_test, compare_key_sid_equal) {
    EXPECT_EQ(format::compare_key_sid("\x01\x00" "abc"sv, "\x01\x00" "abc"sv), 0);
}

// the first byte decides, not the numeric order of storage_ids: 256 (00 01) comes before 1 (01 00)
TEST(log_entry_format_test, compare_key_sid_first_byte) {
    auto sid_256 = "\x00\x01\x00\x00\x00\x00\x00\x00"sv;
    auto sid_1 = "\x01\x00\x00\x00\x00\x00\x00\x00"sv;
    EXPECT_LT(format::compare_key_sid(sid_256, sid_1), 0);
    EXPECT_GT(format::compare_key_sid(sid_1, sid_256), 0);
}

// each byte is compared as unsigned: 0x80 comes after 0x01
TEST(log_entry_format_test, compare_key_sid_unsigned_bytes) {
    EXPECT_LT(format::compare_key_sid("\x01"sv, "\x80"sv), 0);
    EXPECT_LT(format::compare_key_sid("\x7f"sv, "\xff"sv), 0);
    EXPECT_GT(format::compare_key_sid("\xff"sv, "\x00"sv), 0);
}

// if one is a prefix of the other, the shorter one comes first
TEST(log_entry_format_test, compare_key_sid_prefix) {
    EXPECT_LT(format::compare_key_sid("\x01\x00" "ab"sv, "\x01\x00" "abc"sv), 0);
    EXPECT_GT(format::compare_key_sid("\x01\x00" "abc"sv, "\x01\x00" "ab"sv), 0);
}

// the oldest write_version is (0,0) and is older than any other version
TEST(log_entry_format_test, oldest_write_version) {
    using limestone::api::write_version_type;
    const write_version_type oldest = format::oldest_write_version();
    EXPECT_EQ(oldest.get_major(), 0U);
    EXPECT_EQ(oldest.get_minor(), 0U);
    for (const auto& other : {write_version_type{0, 1}, write_version_type{1, 0},
                              write_version_type{UINT64_MAX, UINT64_MAX}}) {
        EXPECT_LT(oldest, other) << other.get_major() << "," << other.get_minor();
    }
}

}  // namespace limestone::testing
