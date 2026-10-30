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
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "test_root.h"

#include "limestone/log/log_entry_bytes.h"
#include "sort/sort_buffer.h"

namespace limestone::testing {

using limestone::internal::log_entry_view;
using limestone::internal::sort_buffer;
using append_result = sort_buffer::append_result;
using entry_type = log_entry_view::entry_type;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;

namespace {

/**
 * @brief Parses the bytes of one entry and creates a view.
 * @param bytes the bytes of one entry. Fails the test if parsing does not succeed
 * @return the view
 */
log_entry_view view_of(std::string_view bytes) {
    auto r = log_entry_view::parse(bytes);
    EXPECT_TRUE(r) << "parse failed";
    return r.view();
}

}  // namespace

// the three data entry types are put, and entries with identical bytes are taken out in the order they were put
TEST(sort_buffer_test, append_and_entry_at) {
    const std::string e1 = normal_entry(1, "k1", "v1", 10, 1);
    const std::string e2 = normal_with_blob(1, "k2", "v2", 10, 2, {100, 200});
    const std::string e3 = remove_entry(2, "k3", 11, 0);

    sort_buffer buf(1024);
    EXPECT_EQ(buf.arena_size_bytes(), 1024U);
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.entry_count(), 0U);

    EXPECT_EQ(buf.append(view_of(e1)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(e2)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(e3)), append_result::appended);
    EXPECT_FALSE(buf.empty());
    EXPECT_EQ(buf.entry_count(), 3U);

    EXPECT_EQ(buf.entry_at(0).serialized_entry(), e1);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), e2);
    EXPECT_EQ(buf.entry_at(2).serialized_entry(), e3);
    EXPECT_EQ(buf.entry_at(0).type(), entry_type::normal_entry);
    EXPECT_EQ(buf.entry_at(1).type(), entry_type::normal_with_blob);
    EXPECT_EQ(buf.entry_at(2).type(), entry_type::remove_entry);
    EXPECT_EQ(buf.entry_at(1).blob_ids(), (std::vector<std::uint64_t>{100, 200}));
    EXPECT_EQ(buf.entry_at(2).write_version().get_major(), 11U);
}

// the view refers to the arena and does not change when the original bytes are destroyed (the bytes are copied)
TEST(sort_buffer_test, entry_is_copied_into_arena) {
    std::string e = normal_entry(1, "key", "value", 10, 1);
    sort_buffer buf(1024);
    ASSERT_EQ(buf.append(view_of(e)), append_result::appended);
    const std::string expected = e;
    e.assign(e.size(), 'x');
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), expected);
    EXPECT_NE(buf.entry_at(0).serialized_entry().data(), e.data());
}

// returns full when the remaining space is not enough for the next entry, and the buffer does not change; an entry
// that exactly fits is put
TEST(sort_buffer_test, full) {
    const std::string e = normal_entry(1, "k", "v", 10, 1);  // 35 bytes
    ASSERT_EQ(e.size(), 35U);

    sort_buffer buf(e.size() * 2);
    EXPECT_EQ(buf.append(view_of(e)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(e)), append_result::appended);  // fills the arena exactly
    EXPECT_EQ(buf.entry_count(), 2U);

    EXPECT_EQ(buf.append(view_of(e)), append_result::full);
    EXPECT_EQ(buf.entry_count(), 2U);
    // even the smallest entry (29 bytes) does not fit
    EXPECT_EQ(buf.append(view_of(remove_entry(1, "", 10, 1))), append_result::full);
    EXPECT_EQ(buf.entry_count(), 2U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), e);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), e);
}

// full when the remaining space is one byte short
TEST(sort_buffer_test, full_by_one_byte) {
    const std::string e = normal_entry(1, "k", "v", 10, 1);
    sort_buffer buf(e.size() * 2 - 1);
    EXPECT_EQ(buf.append(view_of(e)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(e)), append_result::full);
    EXPECT_EQ(buf.entry_count(), 1U);
}

// an entry larger than the whole arena returns oversized whether the arena is empty or partly used, and the buffer
// does not change
TEST(sort_buffer_test, oversized) {
    const std::string small = normal_entry(1, "k", "v", 10, 1);
    const std::string exact = normal_entry(1, "k", std::string(100 - 34, 'v'), 10, 1);
    const std::string large = normal_entry(1, "k", std::string(101 - 34, 'v'), 10, 1);
    ASSERT_EQ(exact.size(), 100U);
    ASSERT_EQ(large.size(), 101U);

    sort_buffer buf(100);
    EXPECT_EQ(buf.append(view_of(large)), append_result::oversized);
    EXPECT_TRUE(buf.empty());

    EXPECT_EQ(buf.append(view_of(small)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(large)), append_result::oversized);
    EXPECT_EQ(buf.entry_count(), 1U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), small);

    // an entry of the same size as the whole arena is not oversized, and is put when the arena is empty
    EXPECT_EQ(buf.append(view_of(exact)), append_result::full);
    buf.clear();
    EXPECT_EQ(buf.append(view_of(exact)), append_result::appended);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), exact);
}

// after clear(), the buffer is empty, and entries are put from the beginning of the reused arena
TEST(sort_buffer_test, clear_and_reuse) {
    const std::string e1 = normal_entry(1, "k1", "v1", 10, 1);
    const std::string e2 = remove_entry(2, "k2", 11, 0);
    sort_buffer buf(e1.size() + e2.size());
    ASSERT_EQ(buf.append(view_of(e1)), append_result::appended);
    ASSERT_EQ(buf.append(view_of(e2)), append_result::appended);
    ASSERT_EQ(buf.append(view_of(e1)), append_result::full);
    const char* arena = buf.entry_at(0).serialized_entry().data();

    buf.clear();
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.entry_count(), 0U);

    EXPECT_EQ(buf.append(view_of(e2)), append_result::appended);
    EXPECT_EQ(buf.append(view_of(e1)), append_result::appended);
    EXPECT_EQ(buf.entry_count(), 2U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), e2);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), e1);
    EXPECT_EQ(buf.entry_at(0).serialized_entry().data(), arena);  // the beginning of the same arena
}

// clear() on an empty buffer does nothing
TEST(sort_buffer_test, clear_empty) {
    sort_buffer buf(64);
    buf.clear();
    EXPECT_TRUE(buf.empty());
    EXPECT_EQ(buf.append(view_of(remove_entry(1, "", 1, 1))), append_result::appended);
}

// the smallest arena (1 byte) can be created; every entry is oversized for it
TEST(sort_buffer_test, smallest_arena) {
    sort_buffer buf(1);
    EXPECT_EQ(buf.arena_size_bytes(), 1U);
    EXPECT_EQ(buf.append(view_of(remove_entry(1, "", 1, 1))), append_result::oversized);
    EXPECT_TRUE(buf.empty());
}

// the arena is not allocated until the first append() (a buffer of the maximum size is created without allocation)
TEST(sort_buffer_test, arena_is_allocated_lazily) {
    const sort_buffer buf(sort_buffer::max_arena_size_bytes);
    EXPECT_EQ(buf.arena_size_bytes(), std::size_t{1} << 32U);
    EXPECT_TRUE(buf.empty());
}

// an entry that is not a data entry and a default-constructed view cannot be appended
TEST(sort_buffer_test, append_non_data_entry_is_fatal) {
    sort_buffer buf(1024);
    EXPECT_DEATH((void) buf.append(view_of(marker(2, 10))), "sort_buffer:append not valid for entry type marker_begin");
    EXPECT_DEATH((void) buf.append(view_of(storage_entry(8, 1, 10, 1))),
                 "sort_buffer:append not valid for entry type add_storage");
    EXPECT_DEATH((void) buf.append(log_entry_view{}), "sort_buffer:append not valid for entry type");
}

// entry_at() out of range, an arena of size 0 and an arena larger than the maximum are FATAL
TEST(sort_buffer_test, out_of_range_and_arena_size_are_fatal) {
    sort_buffer buf(1024);
    EXPECT_DEATH((void) buf.entry_at(0), "sort_buffer:entry_at index 0 is out of range \\(entry count 0\\)");
    ASSERT_EQ(buf.append(view_of(remove_entry(1, "k", 1, 1))), append_result::appended);
    EXPECT_DEATH((void) buf.entry_at(1), "sort_buffer:entry_at index 1 is out of range \\(entry count 1\\)");
    EXPECT_DEATH(sort_buffer{0}, "sort_buffer:sort_buffer arena size 0 must be between 1 and 4294967296");
    EXPECT_DEATH(sort_buffer{sort_buffer::max_arena_size_bytes + 1},
                 "sort_buffer:sort_buffer arena size 4294967297 must be between 1 and 4294967296");
}

namespace {

/**
 * @brief Collects the key_sids of the entries in the buffer in order.
 * @param buf the buffer
 * @return the key_sids
 */
std::vector<std::string> key_sids_of(const sort_buffer& buf) {
    std::vector<std::string> result;
    for (std::size_t i = 0; i < buf.entry_count(); ++i) {
        result.emplace_back(buf.entry_at(i).serialized_key_sid());
    }
    return result;
}

/**
 * @brief Appends the entries in order.
 * @param buf the buffer
 * @param entries the bytes of the entries to append
 */
void append_all(sort_buffer& buf, const std::vector<std::string>& entries) {
    for (const auto& e : entries) {
        ASSERT_EQ(buf.append(view_of(e)), append_result::appended);
    }
}

}  // namespace

// sorted by the bytes of key_sid: the first byte (the lowest byte of the little-endian storage_id) decides first,
// each byte is compared as unsigned, and a prefix comes first
TEST(sort_buffer_test, sort_by_key_sid_bytes) {
    using log_entry_bytes::u64le;
    const std::string s256_a = normal_entry(256, "a", "v", 1, 1);  // key_sid 00 01 00 .. 00 'a'
    const std::string s1_a = normal_entry(1, "a", "v", 1, 1);      // key_sid 01 00 00 .. 00 'a'
    const std::string s1_ab = normal_entry(1, "ab", "v", 1, 1);
    const std::string s1_x80 = remove_entry(1, "\x80", 1, 1);    // 0x80 comes after 'a' (0x61)
    const std::string s1_empty = normal_with_blob(1, "", "v", 1, 1, {7});

    sort_buffer buf(1024);
    append_all(buf, {s1_x80, s1_ab, s1_a, s256_a, s1_empty});
    buf.sort();
    EXPECT_EQ(buf.entry_count(), 5U);
    EXPECT_EQ(key_sids_of(buf), (std::vector<std::string>{
        u64le(256) + "a", u64le(1), u64le(1) + "a", u64le(1) + "ab", u64le(1) + "\x80"}));
}

// entries with the same key_sid are next to each other in descending order of write_version (epoch first, then
// minor); duplicates are not removed
TEST(sort_buffer_test, sort_same_key_sid_by_write_version_descending) {
    const std::string e_epoch10 = normal_entry(1, "k", "epoch10", 10, 0);
    const std::string e_epoch9_minor5 = normal_entry(1, "k", "epoch9 minor5", 9, 5);
    const std::string e_epoch9_minor2 = normal_entry(1, "k", "epoch9 minor2", 9, 2);
    const std::string other = normal_entry(1, "other", "v", 1, 1);

    sort_buffer buf(1024);
    append_all(buf, {e_epoch9_minor2, other, e_epoch10, e_epoch9_minor5});
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 4U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), e_epoch10);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), e_epoch9_minor5);
    EXPECT_EQ(buf.entry_at(2).serialized_entry(), e_epoch9_minor2);
    EXPECT_EQ(buf.entry_at(3).serialized_entry(), other);
}

// remove_entry is sorted by the same rule as the other data entries (the entry type does not affect the order)
TEST(sort_buffer_test, sort_with_remove_entry) {
    const std::string put_old = normal_entry(1, "k1", "v", 1, 1);
    const std::string remove_new = remove_entry(1, "k1", 2, 1);
    const std::string remove_old = remove_entry(1, "k2", 1, 1);
    const std::string put_new = normal_with_blob(1, "k2", "v", 2, 1, {5});

    sort_buffer buf(1024);
    append_all(buf, {put_old, remove_old, remove_new, put_new});
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 4U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), remove_new);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), put_old);
    EXPECT_EQ(buf.entry_at(2).serialized_entry(), put_new);
    EXPECT_EQ(buf.entry_at(3).serialized_entry(), remove_old);
}

// entries with the same key_sid and the same write_version (identical contents) all remain
TEST(sort_buffer_test, sort_identical) {
    const std::string e = normal_entry(1, "k", "v", 1, 1);
    sort_buffer buf(1024);
    append_all(buf, {e, e, e});
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 3U);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(buf.entry_at(i).serialized_entry(), e);
    }
}

// sorting an empty buffer and a buffer with one entry does nothing
TEST(sort_buffer_test, sort_empty_and_single) {
    sort_buffer buf(1024);
    buf.sort();
    EXPECT_TRUE(buf.empty());

    const std::string e = remove_entry(1, "k", 1, 1);
    append_all(buf, {e});
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 1U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), e);
}

// sorting again does not change the result; entries put after clear() are sorted by the same rule
TEST(sort_buffer_test, sort_again_and_after_clear) {
    const std::string a = normal_entry(1, "a", "v", 1, 1);
    const std::string b_new = normal_entry(1, "b", "new", 2, 1);
    const std::string b_old = normal_entry(1, "b", "old", 1, 1);

    sort_buffer buf(1024);
    append_all(buf, {b_old, b_new, a});
    buf.sort();
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 3U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), a);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), b_new);
    EXPECT_EQ(buf.entry_at(2).serialized_entry(), b_old);

    buf.clear();
    append_all(buf, {b_new, a, b_old});
    buf.sort();
    ASSERT_EQ(buf.entry_count(), 3U);
    EXPECT_EQ(buf.entry_at(0).serialized_entry(), a);
    EXPECT_EQ(buf.entry_at(1).serialized_entry(), b_new);
    EXPECT_EQ(buf.entry_at(2).serialized_entry(), b_old);
}

// the names of append_result
TEST(sort_buffer_test, append_result_names) {
    EXPECT_EQ(limestone::internal::to_string_view(append_result::appended), "appended");
    EXPECT_EQ(limestone::internal::to_string_view(append_result::full), "full");
    EXPECT_EQ(limestone::internal::to_string_view(append_result::oversized), "oversized");
    std::ostringstream out;
    out << append_result::full;
    EXPECT_EQ(out.str(), "full");
}

}  // namespace limestone::testing
