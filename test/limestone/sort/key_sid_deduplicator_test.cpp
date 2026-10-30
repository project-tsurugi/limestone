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
#include <string>
#include <string_view>

#include "test_root.h"

#include "limestone/log/log_entry_bytes.h"
#include "sort/key_sid_deduplicator.h"

namespace limestone::testing {

using limestone::internal::key_sid_deduplicator;
using limestone::internal::log_entry_view;

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

// the first entry is accepted; the same key_sid is dropped and a different one is accepted; after a drop, the
// comparison is still with the entry accepted last
TEST(key_sid_deduplicator_test, accept_first_of_each_key_sid) {
    const std::string a_new = normal_entry(1, "a", "new", 2, 0);
    const std::string a_old = normal_entry(1, "a", "old", 1, 0);
    const std::string b = normal_entry(1, "b", "v", 1, 0);
    const std::string c1 = normal_with_blob(1, "c", "v", 3, 0, {7});
    const std::string c2 = remove_entry(1, "c", 2, 0);
    const std::string c3 = normal_entry(1, "c", "v", 1, 0);

    key_sid_deduplicator dedup;
    EXPECT_TRUE(dedup.accept(view_of(a_new)));
    EXPECT_FALSE(dedup.accept(view_of(a_old)));
    EXPECT_TRUE(dedup.accept(view_of(b)));
    EXPECT_TRUE(dedup.accept(view_of(c1)));
    EXPECT_FALSE(dedup.accept(view_of(c2)));
    EXPECT_FALSE(dedup.accept(view_of(c3)));
}

// key_sid includes storage_id: the same key with a different storage_id is a different key_sid
TEST(key_sid_deduplicator_test, storage_id_is_part_of_key_sid) {
    const std::string s1 = normal_entry(1, "k", "v", 1, 0);
    const std::string s2 = normal_entry(2, "k", "v", 1, 0);
    key_sid_deduplicator dedup;
    EXPECT_TRUE(dedup.accept(view_of(s1)));
    EXPECT_TRUE(dedup.accept(view_of(s2)));
}

// entries with the same key_sid and the same write_version (identical entries) are dropped from the second on
TEST(key_sid_deduplicator_test, identical_entries) {
    const std::string e = remove_entry(1, "k", 1, 1);
    key_sid_deduplicator dedup;
    EXPECT_TRUE(dedup.accept(view_of(e)));
    EXPECT_FALSE(dedup.accept(view_of(e)));
    EXPECT_FALSE(dedup.accept(view_of(e)));
}

// a copy of the last accepted key_sid is kept, so destroying the bytes of the input does not change the decision
TEST(key_sid_deduplicator_test, keeps_a_copy_of_the_last_key_sid) {
    std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string a_again = a;
    key_sid_deduplicator dedup;
    EXPECT_TRUE(dedup.accept(view_of(a)));
    a.assign(a.size(), '\xff');
    EXPECT_FALSE(dedup.accept(view_of(a_again)));
}

// a key_sid smaller than the last one (a violation of the ordering contract) is FATAL; storage 256 (first byte 00)
// after storage 1 is smaller
TEST(key_sid_deduplicator_test, out_of_order_is_fatal) {
    const std::string b = normal_entry(1, "b", "v", 1, 0);
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string s256 = normal_entry(256, "b", "v", 1, 0);
    key_sid_deduplicator dedup;
    ASSERT_TRUE(dedup.accept(view_of(b)));
    EXPECT_DEATH((void) dedup.accept(view_of(a)), "key_sid_deduplicator:accept entries are out of order");
    EXPECT_DEATH((void) dedup.accept(view_of(s256)), "key_sid_deduplicator:accept entries are out of order");
}

// an entry that is not a data entry is FATAL
TEST(key_sid_deduplicator_test, non_data_entry_is_fatal) {
    key_sid_deduplicator dedup;
    EXPECT_DEATH((void) dedup.accept(view_of(marker(2, 1))),
                 "key_sid_deduplicator:accept not valid for entry type marker_begin");
    EXPECT_DEATH((void) dedup.accept(view_of(storage_entry(7, 1, 1, 0))),
                 "key_sid_deduplicator:accept not valid for entry type clear_storage");
    EXPECT_DEATH((void) dedup.accept(log_entry_view{}), "key_sid_deduplicator:accept not valid for entry type");
}

}  // namespace limestone::testing
