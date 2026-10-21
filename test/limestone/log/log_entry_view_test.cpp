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

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "test_root.h"

#include "log_entry/log_entry_view.h"
#include "log_entry_bytes.h"

namespace limestone::testing {

using limestone::internal::log_entry_view;
using status = limestone::internal::log_entry_parse_result::status;
using entry_type = limestone::internal::log_entry_format::entry_type;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u32le;
using log_entry_bytes::u64le;
using log_entry_bytes::u8;

// parses a normal_entry and gets the value of each accessor
TEST(log_entry_view_test, parse_normal_entry) {
    std::string bytes = normal_entry(0x0102, "key", "value", 7, 3);
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    EXPECT_EQ(r.consumed(), bytes.size());
    const auto& v = r.view();
    EXPECT_EQ(v.type(), entry_type::normal_entry);
    EXPECT_EQ(v.serialized_entry(), bytes);
    EXPECT_EQ(v.serialized_key_sid(), u64le(0x0102) + "key");
    EXPECT_EQ(v.serialized_value_etc(), u64le(7) + u64le(3) + "value");
    EXPECT_EQ(v.storage(), 0x0102U);
    EXPECT_EQ(v.key(), "key");
    EXPECT_EQ(v.value(), "value");
    EXPECT_EQ(v.write_version().get_major(), 7U);
    EXPECT_EQ(v.write_version().get_minor(), 3U);
}

// parses a normal_entry with an empty key and an empty value
TEST(log_entry_view_test, parse_normal_entry_empty_key_and_value) {
    std::string bytes = normal_entry(1, "", "", 7, 3);
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    const auto& v = r.view();
    EXPECT_EQ(v.key(), "");
    EXPECT_EQ(v.value(), "");
    EXPECT_EQ(v.serialized_key_sid(), u64le(1));
}

// parses a normal_with_blob and gets the blob_ids
TEST(log_entry_view_test, parse_normal_with_blob) {
    std::string bytes = normal_with_blob(2, "key", "value", 7, 3, {1, 0x8000000000000100ULL});
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    EXPECT_EQ(r.consumed(), bytes.size());
    const auto& v = r.view();
    EXPECT_EQ(v.type(), entry_type::normal_with_blob);
    EXPECT_EQ(v.key(), "key");
    EXPECT_EQ(v.value(), "value");
    EXPECT_EQ(v.serialized_value_etc(), u64le(7) + u64le(3) + "value");
    EXPECT_EQ(v.serialized_blob_ids(), u64le(1) + u64le(0x8000000000000100ULL));
    EXPECT_EQ(v.blob_ids(), (std::vector<limestone::api::blob_id_type>{1, 0x8000000000000100ULL}));
}

// parses a normal_with_blob without BLOBs
TEST(log_entry_view_test, parse_normal_with_blob_no_blobs) {
    std::string bytes = normal_with_blob(2, "key", "value", 7, 3, {});
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    const auto& v = r.view();
    EXPECT_EQ(v.serialized_blob_ids(), "");
    EXPECT_TRUE(v.blob_ids().empty());
}

// parses a remove_entry; value_etc is only the write_version
TEST(log_entry_view_test, parse_remove_entry) {
    std::string bytes = remove_entry(3, "key", 7, 3);
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    EXPECT_EQ(r.consumed(), bytes.size());
    const auto& v = r.view();
    EXPECT_EQ(v.type(), entry_type::remove_entry);
    EXPECT_EQ(v.storage(), 3U);
    EXPECT_EQ(v.key(), "key");
    EXPECT_EQ(v.serialized_key_sid(), u64le(3) + "key");
    EXPECT_EQ(v.serialized_value_etc(), u64le(7) + u64le(3));
    EXPECT_EQ(v.write_version().get_major(), 7U);
    EXPECT_EQ(v.write_version().get_minor(), 3U);
}

// parses storage entries; key_sid is only the storage_id and value_etc is only the write_version
TEST(log_entry_view_test, parse_storage_entries) {
    for (std::uint8_t type : {7, 8, 9}) {
        std::string bytes = storage_entry(type, 0x0304, 7, 3);
        auto r = log_entry_view::parse(bytes);
        ASSERT_EQ(r.result(), status::ok) << static_cast<int>(type);
        EXPECT_EQ(r.consumed(), 25U);
        const auto& v = r.view();
        EXPECT_EQ(static_cast<int>(v.type()), type);
        EXPECT_EQ(v.storage(), 0x0304U);
        EXPECT_EQ(v.serialized_key_sid(), u64le(0x0304));
        EXPECT_EQ(v.serialized_value_etc(), u64le(7) + u64le(3));
        EXPECT_EQ(v.write_version().get_major(), 7U);
        EXPECT_EQ(v.write_version().get_minor(), 3U);
    }
}

// parses markers and gets the epoch
TEST(log_entry_view_test, parse_markers) {
    for (std::uint8_t type : {2, 4, 6}) {
        std::string bytes = marker(type, 0x0506);
        auto r = log_entry_view::parse(bytes);
        ASSERT_EQ(r.result(), status::ok) << static_cast<int>(type);
        EXPECT_EQ(r.consumed(), 9U);
        EXPECT_EQ(r.view().epoch_id(), 0x0506U);
    }
    // marker_end; the value of the CRC type is not checked
    for (std::uint8_t crc_type : {1, 0, 0xff}) {
        std::string bytes = marker(3, 0x0506) + u8(crc_type);
        auto r = log_entry_view::parse(bytes);
        ASSERT_EQ(r.result(), status::ok) << static_cast<int>(crc_type);
        EXPECT_EQ(r.consumed(), 10U);
        EXPECT_EQ(r.view().epoch_id(), 0x0506U);
    }
}

// if data follows the entry in the input, consumes and refers to the entry only
TEST(log_entry_view_test, parse_with_trailing_data) {
    std::string entry = normal_entry(1, "key", "value", 7, 3);
    std::string bytes = entry + marker(2, 1);
    auto r = log_entry_view::parse(bytes);
    ASSERT_EQ(r.result(), status::ok);
    EXPECT_EQ(r.consumed(), entry.size());
    EXPECT_EQ(r.view().serialized_entry(), entry);
}

// the conversion to bool is true only if parsing succeeded
TEST(log_entry_view_test, bool_conversion) {
    EXPECT_TRUE(static_cast<bool>(log_entry_view::parse(marker(2, 1))));
    EXPECT_FALSE(static_cast<bool>(log_entry_view::parse(marker(2, 1).substr(0, 4))));  // short_entry
    EXPECT_FALSE(static_cast<bool>(log_entry_view::parse(u8(0))));  // invalid_type
}

// the lower bound is 1 without the type byte
TEST(log_entry_view_test, short_empty) {
    auto r = log_entry_view::parse("");
    EXPECT_EQ(r.result(), status::short_entry);
    EXPECT_EQ(r.required(), 1U);
}

// short normal_entry: 9 up to the length fields, then the size of the whole entry
TEST(log_entry_view_test, short_normal_entry) {
    std::string bytes = normal_entry(1, "key", "value", 7, 3);
    for (std::size_t n = 1; n < 9; ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), 9U) << n;
    }
    for (std::size_t n = 9; n < bytes.size(); ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), bytes.size()) << n;
    }
}

// short normal_with_blob: the end of value_etc + 4 until the number of BLOBs is read, then the size of the whole entry
TEST(log_entry_view_test, short_normal_with_blob) {
    std::string bytes = normal_with_blob(1, "key", "value", 7, 3, {1, 2});
    const std::size_t until_count = 33 + 3 + 5 + 4;
    for (std::size_t n = 1; n < until_count; ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), n < 9 ? 9U : until_count) << n;
    }
    for (std::size_t n = until_count; n < bytes.size(); ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), bytes.size()) << n;
    }
}

// short remove_entry: 5 up to the length field, then the size of the whole entry
TEST(log_entry_view_test, short_remove_entry) {
    std::string bytes = remove_entry(1, "key", 7, 3);
    for (std::size_t n = 1; n < 5; ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), 5U) << n;
    }
    for (std::size_t n = 5; n < bytes.size(); ++n) {
        auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
        EXPECT_EQ(r.result(), status::short_entry) << n;
        EXPECT_EQ(r.required(), bytes.size()) << n;
    }
}

// short entries without length fields: the lower bound is the size of the whole entry
TEST(log_entry_view_test, short_fixed_length_entries) {
    std::string storage = storage_entry(7, 1, 7, 3);
    std::string begin = marker(2, 1);
    std::string end = marker(3, 1) + u8(1);
    for (const auto& bytes : {storage, begin, end}) {
        for (std::size_t n = 1; n < bytes.size(); ++n) {
            auto r = log_entry_view::parse(std::string_view(bytes).substr(0, n));
            EXPECT_EQ(r.result(), status::short_entry) << n;
            EXPECT_EQ(r.required(), bytes.size()) << n;
        }
    }
}

// with the maximum length values, computes the lower bound in 64 bits without allocating
TEST(log_entry_view_test, short_with_maximum_lengths) {
    std::string normal = u8(1) + u32le(0xffffffffU) + u32le(0xffffffffU);
    auto r1 = log_entry_view::parse(normal);
    EXPECT_EQ(r1.result(), status::short_entry);
    EXPECT_EQ(r1.required(), 33U + 2U * 0xffffffffULL);

    std::string blob = u8(10) + u32le(0) + u32le(0) + u64le(1) + u64le(7) + u64le(3) + u32le(0xffffffffU);
    auto r2 = log_entry_view::parse(blob);
    EXPECT_EQ(r2.result(), status::short_entry);
    EXPECT_EQ(r2.required(), 37U + 8U * 0xffffffffULL);
}

// a first byte that is not a valid entry type value gives invalid_type
TEST(log_entry_view_test, invalid_type) {
    for (std::uint8_t value : {0, 11, 0xff}) {
        auto r = log_entry_view::parse(u8(value) + std::string(32, '\0'));
        EXPECT_EQ(r.result(), status::invalid_type) << static_cast<int>(value);
        EXPECT_EQ(r.type_value(), value);
    }
}

// each accessor is valid or invalid for each entry type as specified; an invalid combination aborts
TEST(log_entry_view_test, accessor_validity) {
    using accessor = void (*)(const log_entry_view&);
    const std::vector<std::pair<std::string, accessor>> accessors = {
        {"serialized_entry", [](const log_entry_view& v) { (void) v.serialized_entry(); }},
        {"serialized_key_sid", [](const log_entry_view& v) { (void) v.serialized_key_sid(); }},
        {"serialized_value_etc", [](const log_entry_view& v) { (void) v.serialized_value_etc(); }},
        {"serialized_blob_ids", [](const log_entry_view& v) { (void) v.serialized_blob_ids(); }},
        {"storage", [](const log_entry_view& v) { (void) v.storage(); }},
        {"key", [](const log_entry_view& v) { (void) v.key(); }},
        {"value", [](const log_entry_view& v) { (void) v.value(); }},
        {"write_version", [](const log_entry_view& v) { (void) v.write_version(); }},
        {"blob_ids", [](const log_entry_view& v) { (void) v.blob_ids(); }},
        {"epoch_id", [](const log_entry_view& v) { (void) v.epoch_id(); }},
    };
    struct sample {
        std::string name;
        std::string bytes;  // empty for the default-constructed state
        std::vector<std::string> valid;
    };
    const std::vector<sample> samples = {
        {"normal_entry", normal_entry(1, "key", "value", 7, 3),
         {"serialized_entry", "serialized_key_sid", "serialized_value_etc", "storage", "key", "value", "write_version"}},
        {"normal_with_blob", normal_with_blob(1, "key", "value", 7, 3, {1}),
         {"serialized_entry", "serialized_key_sid", "serialized_value_etc", "serialized_blob_ids", "storage", "key",
          "value", "write_version", "blob_ids"}},
        {"remove_entry", remove_entry(1, "key", 7, 3),
         {"serialized_entry", "serialized_key_sid", "serialized_value_etc", "storage", "key", "write_version"}},
        {"clear_storage", storage_entry(7, 1, 7, 3),
         {"serialized_entry", "serialized_key_sid", "serialized_value_etc", "storage", "write_version"}},
        {"marker_begin", marker(2, 1), {"serialized_entry", "epoch_id"}},
        {"default", "", {}},
    };
    for (const auto& s : samples) {
        log_entry_view v{};
        if (! s.bytes.empty()) {
            auto r = log_entry_view::parse(s.bytes);
            ASSERT_EQ(r.result(), status::ok) << s.name;
            v = r.view();
        }
        for (const auto& [name, call] : accessors) {
            bool valid = std::find(s.valid.begin(), s.valid.end(), name) != s.valid.end();
            if (valid) {
                call(v);
            } else {
                EXPECT_DEATH(call(v), "log_entry_view:" + name + " not valid") << s.name << " " << name;
            }
        }
    }
}

// each accessor of the parse result is valid or invalid for each result kind; an invalid read aborts
TEST(log_entry_view_test, parse_result_accessor_validity) {
    auto ok = log_entry_view::parse(marker(2, 1));
    ASSERT_EQ(ok.result(), status::ok);
    EXPECT_DEATH((void) ok.required(), "log_entry_parse_result:required not valid");
    EXPECT_DEATH((void) ok.type_value(), "log_entry_parse_result:type_value not valid");

    auto short_entry = log_entry_view::parse("");
    ASSERT_EQ(short_entry.result(), status::short_entry);
    EXPECT_DEATH((void) short_entry.view(), "log_entry_parse_result:view not valid");
    EXPECT_DEATH((void) short_entry.consumed(), "log_entry_parse_result:consumed not valid");
    EXPECT_DEATH((void) short_entry.type_value(), "log_entry_parse_result:type_value not valid");

    auto invalid = log_entry_view::parse(u8(0));
    ASSERT_EQ(invalid.result(), status::invalid_type);
    EXPECT_DEATH((void) invalid.view(), "log_entry_parse_result:view not valid");
    EXPECT_DEATH((void) invalid.consumed(), "log_entry_parse_result:consumed not valid");
    EXPECT_DEATH((void) invalid.required(), "log_entry_parse_result:required not valid");
}

// a successful result cannot be created from a default-constructed view
TEST(log_entry_view_test, parse_result_ok_with_default_view_aborts) {
    EXPECT_DEATH((void) limestone::internal::log_entry_parse_result::ok(log_entry_view{}),
                 "log_entry_parse_result:ok default-constructed view");
}

// returns the name of each result kind and "unknown" for an undefined value; operator<< writes the same string
TEST(log_entry_view_test, status_to_string_view) {
    const std::vector<std::pair<status, std::string_view>> cases{
        {status::ok, "ok"},
        {status::short_entry, "short_entry"},
        {status::invalid_type, "invalid_type"},
        {static_cast<status>(99), "unknown"},
    };
    for (const auto& [result, name] : cases) {
        EXPECT_EQ(limestone::internal::to_string_view(result), name);
        std::ostringstream out;
        out << result;
        EXPECT_EQ(out.str(), name);
    }
}

}  // namespace limestone::testing
