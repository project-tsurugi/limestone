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
#include <cstdio>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "log_entry.h"
#include "log_entry_bytes.h"

namespace limestone::testing {

using limestone::api::log_entry;
using limestone::internal::log_entry_view;
using entry_type = log_entry::entry_type;
using read_error = log_entry::read_error;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u8;

namespace {

// a short entry that fits in SSO and a long one that does not; covers both the case where copy and move change the
// location of the bytes and the case where they do not
const std::string short_value = "v";
const std::string long_value(200, 'x');

/**
 * @brief Parses bytes and creates an owning entry.
 * @param bytes the bytes of one entry
 * @return the created entry
 */
log_entry make_entry(std::string_view bytes) {
    auto r = log_entry_view::parse(bytes);
    EXPECT_TRUE(r);
    return log_entry{r.view()};
}

/**
 * @brief Checks that the values of the entry match normal_with_blob(2, "key", value, 7, 3, {11, 12}).
 * @param e the entry to check
 * @param value the expected value
 */
void expect_blob_entry(const log_entry& e, const std::string& value) {
    EXPECT_EQ(e.type(), entry_type::normal_with_blob);
    EXPECT_EQ(e.view().serialized_entry(), normal_with_blob(2, "key", value, 7, 3, {11, 12}));
    EXPECT_EQ(e.storage(), 2U);
    std::string key;
    e.key(key);
    EXPECT_EQ(key, "key");
    std::string v;
    e.value(v);
    EXPECT_EQ(v, value);
    limestone::api::write_version_type wv;
    e.write_version(wv);
    EXPECT_EQ(wv.get_major(), 7U);
    EXPECT_EQ(wv.get_minor(), 3U);
    EXPECT_EQ(e.get_blob_ids(), (std::vector<limestone::api::blob_id_type>{11, 12}));
    EXPECT_EQ(e.key_sid(), log_entry_bytes::u64le(2) + "key");
    EXPECT_EQ(e.value_etc(), log_entry_bytes::u64le(7) + log_entry_bytes::u64le(3) + value);
    EXPECT_EQ(e.raw_blob_ids(), log_entry_bytes::u64le(11) + log_entry_bytes::u64le(12));
    // the accessors with the new names return the same values
    EXPECT_EQ(e.serialized_entry(), e.view().serialized_entry());
    EXPECT_EQ(e.serialized_key_sid(), log_entry_bytes::u64le(2) + "key");
    EXPECT_EQ(e.serialized_value_etc(), log_entry_bytes::u64le(7) + log_entry_bytes::u64le(3) + value);
    EXPECT_EQ(e.serialized_blob_ids(), log_entry_bytes::u64le(11) + log_entry_bytes::u64le(12));
    EXPECT_EQ(e.key(), "key");
    EXPECT_EQ(e.value(), value);
    EXPECT_EQ(e.write_version().get_major(), 7U);
    EXPECT_EQ(e.write_version().get_minor(), 3U);
    EXPECT_EQ(e.blob_ids(), (std::vector<limestone::api::blob_id_type>{11, 12}));
}

/**
 * @brief Checks that the entry is in the default-constructed state.
 * @param e the entry to check
 */
void expect_default_state(const log_entry& e) {
    EXPECT_EQ(e.type(), entry_type::this_id_is_not_used);
    EXPECT_EQ(e.view().type(), entry_type::this_id_is_not_used);
}

/**
 * @brief Reads one entry from a stream of bytes.
 * @param bytes the content of the stream
 * @param e the destination
 * @param ec the result of reading
 * @return the return value of read_entry_from
 */
bool read_from(const std::string& bytes, log_entry& e, read_error& ec) {
    std::istringstream in(bytes);
    return e.read_entry_from(in, ec);
}

/**
 * @brief Calls a function that writes to a temporary file and returns the bytes written.
 * @param write a function that takes the FILE* of the temporary file and writes to it
 * @return the bytes written
 */
template <class Write>
std::string written_by(Write&& write) {
    FILE* fp = std::tmpfile();
    if (fp == nullptr) {
        ADD_FAILURE() << "tmpfile failed";
        return {};
    }
    std::forward<Write>(write)(fp);
    const long size = std::ftell(fp);
    std::rewind(fp);
    std::string written(static_cast<std::size_t>(size), '\0');
    written.resize(std::fread(written.data(), 1, written.size(), fp));
    std::fclose(fp);
    return written;
}

/**
 * @brief Writes an entry to a temporary file with write_serialized() and returns the bytes written.
 * @param bytes the bytes of the entry to write
 * @param handling how to write the write_version
 * @return the bytes written
 */
std::string write_serialized_to_string(const std::string& bytes, log_entry::write_version_handling handling) {
    auto r = log_entry_view::parse(bytes);
    EXPECT_TRUE(r);
    return written_by([&](FILE* fp) { log_entry::write_serialized(fp, r.view(), handling); });
}

}  // namespace

// in the default-constructed state, the type is this_id_is_not_used and the view is also default-constructed
TEST(log_entry_test, default_constructed) {
    log_entry e;
    expect_default_state(e);
}

// created from a view, the entry copies the region into its own buffer and returns values after the region is gone
TEST(log_entry_test, construct_from_view) {
    for (const auto& value : {short_value, long_value}) {
        auto bytes = std::make_unique<std::string>(normal_with_blob(2, "key", value, 7, 3, {11, 12}));
        auto r = log_entry_view::parse(*bytes);
        ASSERT_TRUE(r);
        log_entry e{r.view()};
        EXPECT_NE(e.view().serialized_entry().data(), bytes->data());
        bytes.reset();
        expect_blob_entry(e, value);
    }
}

// cannot be created from a default-constructed view
TEST(log_entry_test, construct_from_default_view) {
    EXPECT_DEATH(log_entry{log_entry_view{}}, "log_entry_view:serialized_entry not valid");
}

// a copy has a view of its own buffer and returns values after the source is destroyed
TEST(log_entry_test, copy_construct) {
    for (const auto& value : {short_value, long_value}) {
        auto src = std::make_unique<log_entry>(make_entry(normal_with_blob(2, "key", value, 7, 3, {11, 12})));
        log_entry dst{*src};
        EXPECT_NE(dst.view().serialized_entry().data(), src->view().serialized_entry().data());
        expect_blob_entry(*src, value);
        src.reset();
        expect_blob_entry(dst, value);
    }
}

// after copy assignment, the target has a view of its own buffer and returns values after the source is destroyed
TEST(log_entry_test, copy_assign) {
    for (const auto& value : {short_value, long_value}) {
        auto src = std::make_unique<log_entry>(make_entry(normal_with_blob(2, "key", value, 7, 3, {11, 12})));
        log_entry dst = make_entry(normal_entry(1, "other", "other", 1, 1));
        dst = *src;
        EXPECT_NE(dst.view().serialized_entry().data(), src->view().serialized_entry().data());
        src.reset();
        expect_blob_entry(dst, value);
    }
}

// copy assignment to itself does not change the entry
TEST(log_entry_test, copy_assign_self) {
    log_entry e = make_entry(normal_with_blob(2, "key", short_value, 7, 3, {11, 12}));
    const log_entry& self = e;
    e = self;
    expect_blob_entry(e, short_value);
}

// move construction: the target returns values and the source is left in the default-constructed state
TEST(log_entry_test, move_construct) {
    for (const auto& value : {short_value, long_value}) {
        auto src = std::make_unique<log_entry>(make_entry(normal_with_blob(2, "key", value, 7, 3, {11, 12})));
        log_entry dst{std::move(*src)};
        expect_default_state(*src);  // NOLINT(bugprone-use-after-move)
        src.reset();
        expect_blob_entry(dst, value);
    }
}

// move assignment: the target returns values and the source is left in the default-constructed state
TEST(log_entry_test, move_assign) {
    for (const auto& value : {short_value, long_value}) {
        auto src = std::make_unique<log_entry>(make_entry(normal_with_blob(2, "key", value, 7, 3, {11, 12})));
        log_entry dst = make_entry(normal_entry(1, "other", "other", 1, 1));
        dst = std::move(*src);
        expect_default_state(*src);  // NOLINT(bugprone-use-after-move)
        src.reset();
        expect_blob_entry(dst, value);
    }
}

// move assignment to itself does not change the entry
TEST(log_entry_test, move_assign_self) {
    log_entry e = make_entry(normal_with_blob(2, "key", short_value, 7, 3, {11, 12}));
    log_entry& self = e;
    e = std::move(self);
    expect_blob_entry(e, short_value);  // NOLINT(bugprone-use-after-move)
}

// copies and moves of a default-constructed entry are in the default-constructed state
TEST(log_entry_test, copy_and_move_default_state) {
    const log_entry src{};
    log_entry copied{src};
    expect_default_state(copied);
    log_entry assigned = make_entry(normal_entry(1, "other", "other", 1, 1));
    assigned = src;
    expect_default_state(assigned);
    log_entry moved{std::move(copied)};
    expect_default_state(moved);
    log_entry move_assigned = make_entry(normal_entry(1, "other", "other", 1, 1));
    move_assigned = std::move(assigned);
    expect_default_state(move_assigned);
}

// reading an entry of each type holds exactly the bytes read
TEST(log_entry_test, read_each_type) {
    const std::vector<std::string> entries{
        normal_entry(1, "key", "value", 7, 3),
        normal_with_blob(2, "key", "value", 7, 3, {11, 12}),
        normal_with_blob(2, "key", "value", 7, 3, {}),
        remove_entry(3, "key", 7, 3),
        storage_entry(7, 4, 7, 3),
        storage_entry(8, 5, 7, 3),
        storage_entry(9, 6, 7, 3),
        marker(2, 10),
        marker(3, 11) + u8(1),
        marker(4, 12),
        marker(6, 13),
    };
    for (const auto& bytes : entries) {
        log_entry e;
        read_error ec;
        EXPECT_TRUE(read_from(bytes, e, ec));
        EXPECT_FALSE(ec);
        EXPECT_EQ(e.type(), static_cast<entry_type>(bytes[0]));
        EXPECT_EQ(e.view().serialized_entry(), bytes);
    }
}

// returns the epoch of a marker
TEST(log_entry_test, read_marker_epoch) {
    log_entry e;
    read_error ec;
    ASSERT_TRUE(read_from(marker(4, 0x0102030405060708ULL), e, ec));
    EXPECT_EQ(e.epoch_id(), 0x0102030405060708ULL);
}

// reading again into the same object leaves no values of the previous entry
TEST(log_entry_test, read_sequence) {
    std::string first = normal_with_blob(2, "key", long_value, 7, 3, {11, 12});
    std::string second = remove_entry(3, "k", 8, 4);
    std::istringstream in(first + second);
    log_entry e;
    read_error ec;
    ASSERT_TRUE(e.read_entry_from(in, ec));
    EXPECT_EQ(e.view().serialized_entry(), first);
    ASSERT_TRUE(e.read_entry_from(in, ec));
    EXPECT_EQ(e.view().serialized_entry(), second);
    EXPECT_FALSE(e.read_entry_from(in, ec));
    EXPECT_FALSE(ec);
    expect_default_state(e);
}

// at the end of the stream, returns false without an error and leaves the default-constructed state
TEST(log_entry_test, read_eof) {
    log_entry e = make_entry(normal_entry(1, "key", "value", 7, 3));
    read_error ec;
    EXPECT_FALSE(read_from("", e, ec));
    EXPECT_FALSE(ec);
    expect_default_state(e);
}

// a truncated entry gives short_entry with its type in ec, and leaves the default-constructed state
TEST(log_entry_test, read_short_entry) {
    const std::vector<std::string> entries{
        normal_entry(1, "key", "value", 7, 3),
        normal_with_blob(2, "key", "value", 7, 3, {11, 12}),
        remove_entry(3, "key", 7, 3),
        storage_entry(7, 4, 7, 3),
        marker(3, 11) + u8(1),
    };
    for (const auto& bytes : entries) {
        for (std::size_t n = 1; n < bytes.size(); ++n) {
            log_entry e = make_entry(normal_entry(1, "other", "other", 1, 1));
            read_error ec;
            EXPECT_FALSE(read_from(bytes.substr(0, n), e, ec)) << n;
            EXPECT_EQ(ec.value(), read_error::short_entry) << n;
            EXPECT_EQ(ec.entry_type(), static_cast<entry_type>(bytes[0])) << n;
            expect_default_state(e);
        }
    }
}

// an undefined type value gives unknown_type with the value in ec, and leaves the default-constructed state
TEST(log_entry_test, read_unknown_type) {
    for (std::uint8_t value : {0, 11, 0xff}) {
        log_entry e = make_entry(normal_entry(1, "other", "other", 1, 1));
        read_error ec;
        EXPECT_FALSE(read_from(u8(value) + std::string(32, '\0'), e, ec));
        EXPECT_EQ(ec.value(), read_error::unknown_type);
        EXPECT_EQ(ec.entry_type(), static_cast<entry_type>(value));
        expect_default_state(e);
    }
}

// emptying the value of normal_entry sets the value length to 0 and keeps the other parts
TEST(log_entry_test, truncate_normal_entry) {
    log_entry e = make_entry(normal_entry(1, "key", long_value, 7, 3));
    e.truncate_value_from_normal_entry();
    EXPECT_EQ(e.view().serialized_entry(), normal_entry(1, "key", "", 7, 3));
}

// emptying the value of normal_with_blob sets the value length to 0 and keeps the blob_ids
TEST(log_entry_test, truncate_normal_with_blob) {
    log_entry e = make_entry(normal_with_blob(2, "key", long_value, 7, 3, {11, 12}));
    e.truncate_value_from_normal_entry();
    EXPECT_EQ(e.view().serialized_entry(), normal_with_blob(2, "key", "", 7, 3, {11, 12}));
    EXPECT_EQ(e.get_blob_ids(), (std::vector<limestone::api::blob_id_type>{11, 12}));
}

// entry types without a value do not change
TEST(log_entry_test, truncate_other_types) {
    const std::vector<std::string> entries{
        remove_entry(3, "key", 7, 3),
        storage_entry(7, 4, 7, 3),
        marker(2, 10),
    };
    for (const auto& bytes : entries) {
        log_entry e = make_entry(bytes);
        e.truncate_value_from_normal_entry();
        EXPECT_EQ(e.view().serialized_entry(), bytes);
    }
}

// does nothing in the default-constructed state
TEST(log_entry_test, truncate_default_state) {
    log_entry e;
    e.truncate_value_from_normal_entry();
    expect_default_state(e);
}

// write writes the bytes as is, including the CRC type of marker_end as read
TEST(log_entry_test, write_entry_as_is) {
    const std::vector<std::string> entries{
        normal_with_blob(2, "key", long_value, 7, 3, {11, 12}),
        marker(3, 11) + u8(0x7f),
    };
    for (const auto& bytes : entries) {
        log_entry e = make_entry(bytes);
        EXPECT_EQ(written_by([&](FILE* fp) { e.write(fp); }), bytes);
    }
}

// cannot write the default-constructed state
TEST(log_entry_test, write_default_state) {
    log_entry e;
    EXPECT_DEATH(e.write(stdout), "log_entry_view:serialized_entry not valid");
}

// the old accessors follow the type checks of the view and abort when asked for a value that is not valid
TEST(log_entry_test, accessor_not_valid_for_type) {
    log_entry normal = make_entry(normal_entry(1, "key", "value", 7, 3));
    EXPECT_DEATH((void) normal.get_blob_ids(), "log_entry_view:blob_ids not valid");
    EXPECT_DEATH((void) normal.raw_blob_ids(), "log_entry_view:serialized_blob_ids not valid");
    EXPECT_DEATH((void) normal.epoch_id(), "log_entry_view:epoch_id not valid");
    log_entry storage = make_entry(storage_entry(7, 4, 7, 3));
    std::string buf;
    EXPECT_DEATH(storage.key(buf), "log_entry_view:key not valid");
    log_entry removed = make_entry(remove_entry(3, "key", 7, 3));
    EXPECT_DEATH(removed.value(buf), "log_entry_view:value not valid");
    log_entry begin = make_entry(marker(2, 10));
    EXPECT_DEATH((void) begin.key_sid(), "log_entry_view:serialized_key_sid not valid");
    EXPECT_DEATH((void) begin.value_etc(), "log_entry_view:serialized_value_etc not valid");
    EXPECT_DEATH((void) begin.storage(), "log_entry_view:storage not valid");
    limestone::api::write_version_type wv;
    EXPECT_DEATH(begin.write_version(wv), "log_entry_view:write_version not valid");
}

// the accessors with the new names also abort when asked for a value that is not valid
TEST(log_entry_test, new_accessor_not_valid_for_type) {
    log_entry normal = make_entry(normal_entry(1, "key", "value", 7, 3));
    EXPECT_DEATH((void) normal.blob_ids(), "log_entry_view:blob_ids not valid");
    EXPECT_DEATH((void) normal.serialized_blob_ids(), "log_entry_view:serialized_blob_ids not valid");
    log_entry storage = make_entry(storage_entry(7, 4, 7, 3));
    EXPECT_DEATH((void) storage.key(), "log_entry_view:key not valid");
    log_entry removed = make_entry(remove_entry(3, "key", 7, 3));
    EXPECT_DEATH((void) removed.value(), "log_entry_view:value not valid");
    log_entry begin = make_entry(marker(2, 10));
    EXPECT_DEATH((void) begin.serialized_key_sid(), "log_entry_view:serialized_key_sid not valid");
    EXPECT_DEATH((void) begin.serialized_value_etc(), "log_entry_view:serialized_value_etc not valid");
    EXPECT_DEATH((void) begin.write_version(), "log_entry_view:write_version not valid");
    const log_entry empty{};
    EXPECT_DEATH((void) empty.serialized_entry(), "log_entry_view:serialized_entry not valid");
}

// each factory creates an entry with the bytes in the format
TEST(log_entry_test, factories) {
    using limestone::api::write_version_type;
    const write_version_type wv{7, 3};
    const std::vector<std::pair<log_entry, std::string>> cases{
        {log_entry::make_normal_entry(1, "key", "value", wv), normal_entry(1, "key", "value", 7, 3)},
        {log_entry::make_normal_entry(1, "", "", wv), normal_entry(1, "", "", 7, 3)},
        {log_entry::make_normal_entry(1, "key", long_value, wv), normal_entry(1, "key", long_value, 7, 3)},
        {log_entry::make_normal_with_blob(2, "key", "value", wv, {11, 12}),
         normal_with_blob(2, "key", "value", 7, 3, {11, 12})},
        {log_entry::make_normal_with_blob(2, "key", "value", wv, {}), normal_with_blob(2, "key", "value", 7, 3, {})},
        {log_entry::make_remove_entry(3, "key", wv), remove_entry(3, "key", 7, 3)},
        {log_entry::make_clear_storage(4, wv), storage_entry(7, 4, 7, 3)},
        {log_entry::make_add_storage(5, wv), storage_entry(8, 5, 7, 3)},
        {log_entry::make_remove_storage(6, wv), storage_entry(9, 6, 7, 3)},
        {log_entry::make_marker_begin(10), marker(2, 10)},
        {log_entry::make_marker_end(11), marker(3, 11) + u8(1)},
        {log_entry::make_marker_durable(12), marker(4, 12)},
        {log_entry::make_marker_invalidated_begin(13), marker(6, 13)},
    };
    for (const auto& [e, bytes] : cases) {
        EXPECT_EQ(e.type(), static_cast<entry_type>(bytes[0]));
        EXPECT_EQ(e.view().serialized_entry(), bytes) << e.type();
    }
}

// aborts if the length of the key or the value does not fit in 32 bits
TEST(log_entry_test, factories_length_exceeds_32bit) {
    // the factories check the lengths and abort before reading the bytes, so a string_view that refers to a small
    // region with a large length is enough
    const char buf[1]{};
    const std::string_view huge{buf, std::size_t{UINT32_MAX} + 1};
    const limestone::api::write_version_type wv{7, 3};
    EXPECT_DEATH((void) log_entry::make_normal_entry(1, huge, "", wv), "key length");
    EXPECT_DEATH((void) log_entry::make_normal_entry(1, "", huge, wv), "value length");
    EXPECT_DEATH((void) log_entry::make_normal_with_blob(1, huge, "", wv, {}), "key length");
    EXPECT_DEATH((void) log_entry::make_normal_with_blob(1, "", huge, wv, {}), "value length");
    EXPECT_DEATH((void) log_entry::make_remove_entry(1, huge, wv), "key length");
}

// the typed writers throw limestone_exception without writing any byte if the length of the key or the value, or
// the number of BLOBs, does not fit in 32 bits
TEST(log_entry_test, typed_writers_length_exceeds_32bit) {
    // the lengths are checked before writing, so a string_view that refers to a small region with a large length is
    // enough. The number of BLOBs is not checked here, because it would need a vector of 2^32 elements
    const char buf[1]{};
    const std::string_view huge{buf, std::size_t{UINT32_MAX} + 1};
    const limestone::api::write_version_type wv{7, 3};
    const std::vector<std::pair<std::function<void(FILE*)>, std::string>> cases{
        {[&](FILE* fp) { log_entry::write(fp, 1, huge, "", wv); }, "key length"},
        {[&](FILE* fp) { log_entry::write(fp, 1, "", huge, wv); }, "value length"},
        {[&](FILE* fp) { log_entry::write_with_blob(fp, 1, huge, "", wv, {11}); }, "key length"},
        {[&](FILE* fp) { log_entry::write_with_blob(fp, 1, "", huge, wv, {11}); }, "value length"},
        {[&](FILE* fp) { log_entry::write_remove(fp, 1, huge, wv); }, "key length"},
    };
    for (const auto& [write, message] : cases) {
        const std::string written = written_by([&](FILE* fp) {
            try {
                write(fp);
                ADD_FAILURE() << "no exception: " << message;
            } catch (const limestone::api::limestone_exception& e) {
                EXPECT_NE(std::string_view{e.what()}.find(message), std::string_view::npos) << e.what();
            }
        });
        EXPECT_TRUE(written.empty()) << message;
    }
}

// keep writes the bytes as is
TEST(log_entry_test, write_serialized_keep) {
    const std::vector<std::string> entries{
        normal_entry(1, "key", long_value, 7, 3),
        normal_with_blob(2, "key", "value", 7, 3, {11, 12}),
        remove_entry(3, "key", 7, 3),
        storage_entry(7, 4, 7, 3),
        marker(2, 10),
        marker(3, 11) + u8(0x7f),
    };
    for (const auto& bytes : entries) {
        EXPECT_EQ(write_serialized_to_string(bytes, log_entry::write_version_handling::keep), bytes);
    }
}

// oldest writes the bytes with only the write_version replaced by (0,0)
TEST(log_entry_test, write_serialized_oldest) {
    const std::vector<std::pair<std::string, std::string>> cases{
        {normal_entry(1, "key", long_value, 7, 3), normal_entry(1, "key", long_value, 0, 0)},
        {normal_entry(1, "", "", 7, 3), normal_entry(1, "", "", 0, 0)},
        {normal_with_blob(2, "key", "value", 7, 3, {11, 12}), normal_with_blob(2, "key", "value", 0, 0, {11, 12})},
        {remove_entry(3, "key", 7, 3), remove_entry(3, "key", 0, 0)},
        {storage_entry(7, 4, 7, 3), storage_entry(7, 4, 0, 0)},
    };
    for (const auto& [bytes, expected] : cases) {
        EXPECT_EQ(write_serialized_to_string(bytes, log_entry::write_version_handling::oldest), expected);
    }
}

// oldest aborts for a marker, which has no write_version
TEST(log_entry_test, write_serialized_oldest_marker) {
    EXPECT_DEATH((void) write_serialized_to_string(marker(2, 10), log_entry::write_version_handling::oldest),
                 "log_entry_view:serialized_value_etc not valid");
}

// cannot write a default-constructed view
TEST(log_entry_test, write_serialized_default_view) {
    EXPECT_DEATH(log_entry::write_serialized(stdout, log_entry_view{}), "log_entry_view:serialized_entry not valid");
}

// throws limestone_io_exception if writing fails
TEST(log_entry_test, write_serialized_io_error) {
    const std::string bytes = normal_entry(1, "key", "value", 7, 3);
    auto r = log_entry_view::parse(bytes);
    ASSERT_TRUE(r);
    FILE* fp = std::fopen("/dev/null", "r");  // opened read-only, so writing fails
    ASSERT_NE(fp, nullptr);
    EXPECT_THROW(log_entry::write_serialized(fp, r.view()), limestone::limestone_io_exception);
    std::fclose(fp);
}

// the output of write_serialized matches the output of the typed writers
TEST(log_entry_test, write_serialized_matches_typed_writers) {
    using limestone::api::write_version_type;
    const write_version_type wv{7, 3};
    const std::vector<limestone::api::blob_id_type> blob_ids{11, 12};
    const std::vector<std::pair<log_entry, std::string>> cases{
        {log_entry::make_normal_entry(1, "key", long_value, wv),
         written_by([&](FILE* fp) { log_entry::write(fp, 1, "key", long_value, wv); })},
        {log_entry::make_normal_with_blob(2, "key", "value", wv, blob_ids),
         written_by([&](FILE* fp) { log_entry::write_with_blob(fp, 2, "key", "value", wv, blob_ids); })},
        {log_entry::make_remove_entry(3, "key", wv),
         written_by([&](FILE* fp) { log_entry::write_remove(fp, 3, "key", wv); })},
        {log_entry::make_clear_storage(4, wv), written_by([&](FILE* fp) { log_entry::write_clear_storage(fp, 4, wv); })},
        {log_entry::make_add_storage(5, wv), written_by([&](FILE* fp) { log_entry::write_add_storage(fp, 5, wv); })},
        {log_entry::make_remove_storage(6, wv),
         written_by([&](FILE* fp) { log_entry::write_remove_storage(fp, 6, wv); })},
        {log_entry::make_marker_begin(10), written_by([](FILE* fp) { log_entry::begin_session(fp, 10); })},
        {log_entry::make_marker_end(11), written_by([](FILE* fp) { log_entry::end_session(fp, 11); })},
        {log_entry::make_marker_durable(12), written_by([](FILE* fp) { log_entry::durable_epoch(fp, 12); })},
        {log_entry::make_marker_invalidated_begin(13),
         written_by([](FILE* fp) { log_entry::invalidated_begin(fp, 13); })},
    };
    for (const auto& [e, typed] : cases) {
        EXPECT_EQ(written_by([&](FILE* fp) { log_entry::write_serialized(fp, e.view()); }), typed) << e.type();
    }
}

}  // namespace limestone::testing
