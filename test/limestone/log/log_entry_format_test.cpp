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

}  // namespace limestone::testing
