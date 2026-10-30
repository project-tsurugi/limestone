/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <memory>
#include <vector>
#include "partitioned_cursor/partitioned_cursor_impl.h"
#include "partitioned_cursor/cursor_entry_queue.h"
#include "partitioned_cursor/end_marker.h"
#include "log_entry/log_entry.h"
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

namespace limestone::testing {

using limestone::internal::partitioned_cursor_impl;
using limestone::internal::cursor_entry_queue;
using limestone::internal::end_marker;
using limestone::api::log_entry;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

class partitioned_cursor_impl_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Create a log_entry instance of a normal entry.
    std::vector<log_entry> create_singleton_log_entry_batch(storage_id_type storage, const std::string& key, const std::string& value,
                                                   const write_version_type& wversion) {
        return {log_entry::make_normal_entry(storage, key, value, wversion)};  // Return as vector<log_entry>
    }

    // Create a batch of log_entry instances from a list of tuples.
    std::vector<log_entry> create_log_entry_batch(std::initializer_list<std::tuple<storage_id_type, std::string, std::string, write_version_type>> entries) {
        std::vector<log_entry> result;
        for (const auto& [sid, key, value, wv] : entries) {
            result.emplace_back(create_singleton_log_entry_batch(sid, key, value, wv).at(0));
        }
        return result;
    }
};

TEST_F(partitioned_cursor_impl_test, single_entry_and_end_marker) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    // Push one log_entry
    EXPECT_TRUE(queue->push(create_singleton_log_entry_batch(123, "key", "value", {12, 34})));

    // Push end_marker
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    // First next() should return true
    EXPECT_TRUE(cursor.next());
    EXPECT_EQ(cursor.storage(), 123);
    EXPECT_EQ(cursor.type(), log_entry::entry_type::normal_entry);
    std::string key;
    cursor.key(key);
    EXPECT_EQ(key, "key");
    std::string value;
    cursor.value(value);
    EXPECT_EQ(value, "value");

    // Second next() should return false (end_marker received)
    EXPECT_FALSE(cursor.next());
}

TEST_F(partitioned_cursor_impl_test, next_returns_false_after_cursor_closed) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    // Push multiple log_entries
    EXPECT_TRUE(queue->push(create_singleton_log_entry_batch(123, "key1", "value1", {12, 34})));
    EXPECT_TRUE(queue->push(create_singleton_log_entry_batch(456, "key2", "value2", {56, 78})));

    cursor.close();
    EXPECT_FALSE(cursor.next());
}

TEST_F(partitioned_cursor_impl_test, current_returns_last_entry_after_next) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    auto entry1 = create_singleton_log_entry_batch(123, "k1", "v1", {1, 0});
    auto entry2 = create_singleton_log_entry_batch(456, "k2", "v2", {2, 0});
    EXPECT_TRUE(queue->push(entry1));
    EXPECT_TRUE(queue->push(entry2));
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    ASSERT_TRUE(cursor.next());
    const log_entry& current1 = cursor.current();
    EXPECT_EQ(current1.storage(), 123);
    EXPECT_EQ(current1.key(), "k1");
    EXPECT_EQ(current1.value(), "v1");

    ASSERT_TRUE(cursor.next());
    const log_entry& current2 = cursor.current();
    EXPECT_EQ(current2.storage(), 456);
    EXPECT_EQ(current2.key(), "k2");
    EXPECT_EQ(current2.value(), "v2");

    EXPECT_FALSE(cursor.next());
}

// Verify that blob_ids() returns the BLOB IDs of a normal_with_blob entry
TEST_F(partitioned_cursor_impl_test, blob_ids_of_normal_with_blob) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    std::vector<log_entry> batch{log_entry::make_normal_with_blob(123, "k1", "v1", {1, 0}, {11, 12})};
    EXPECT_TRUE(queue->push(batch));
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    ASSERT_TRUE(cursor.next());
    EXPECT_EQ(cursor.type(), log_entry::entry_type::normal_with_blob);
    EXPECT_EQ(cursor.blob_ids(), (std::vector<limestone::api::blob_id_type>{11, 12}));
    EXPECT_FALSE(cursor.next());
}

// Verify that after moving from current(), the current entry is in the default-constructed state until next()
TEST_F(partitioned_cursor_impl_test, current_after_move_is_default_constructed) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    EXPECT_TRUE(queue->push(create_log_entry_batch({{123, "k1", "v1", {1, 0}}, {456, "k2", "v2", {2, 0}}})));
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    ASSERT_TRUE(cursor.next());
    log_entry taken = std::move(cursor.current());
    EXPECT_EQ(taken.key(), "k1");
    EXPECT_EQ(cursor.type(), log_entry::entry_type::this_id_is_not_used);
    EXPECT_DEATH((void) cursor.storage(), "log_entry_view:storage not valid");

    // next() moves to the next entry as usual
    ASSERT_TRUE(cursor.next());
    EXPECT_EQ(cursor.current().key(), "k2");
    EXPECT_FALSE(cursor.next());
}

TEST_F(partitioned_cursor_impl_test, create_cursor_returns_valid_cursor) {
    auto queue = std::make_shared<cursor_entry_queue>(8);

    // Use create_cursor to create a wrapped API cursor
    auto cursor = partitioned_cursor_impl::create_cursor(queue);
    ASSERT_NE(cursor, nullptr);  // cursor should not be null

    // Push a log entry
    EXPECT_TRUE(queue->push(create_singleton_log_entry_batch(42, "foo", "bar", {1, 2})));
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    // The returned cursor behaves like any other cursor
    EXPECT_TRUE(cursor->next());
    EXPECT_EQ(cursor->storage(), 42);
    std::string key, value;
    cursor->key(key);
    cursor->value(value);
    EXPECT_EQ(key, "foo");
    EXPECT_EQ(value, "bar");

    EXPECT_FALSE(cursor->next());
}

TEST_F(partitioned_cursor_impl_test, batch_with_multiple_entries_is_iterated) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    auto batch = create_log_entry_batch({
        {1, "a", "A", {10, 0}},
        {2, "b", "B", {20, 0}},
        {3, "c", "C", {30, 0}},
    });
    EXPECT_TRUE(queue->push(batch));
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    std::vector<std::tuple<storage_id_type, std::string, std::string>> expected = {
        {1, "a", "A"},
        {2, "b", "B"},
        {3, "c", "C"},
    };

    for (const auto& [expected_sid, expected_key, expected_value] : expected) {
        ASSERT_TRUE(cursor.next());
        EXPECT_EQ(cursor.storage(), expected_sid);

        std::string actual_key, actual_value;
        cursor.key(actual_key);
        cursor.value(actual_value);
        EXPECT_EQ(actual_key, expected_key);
        EXPECT_EQ(actual_value, expected_value);
    }

    EXPECT_FALSE(cursor.next());  // Reached end_marker, expect false

}

TEST_F(partitioned_cursor_impl_test, multiple_batches_are_processed_in_sequence) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    // Push two batches in sequence

    EXPECT_TRUE(queue->push(create_log_entry_batch({
        {10, "x", "X", {100, 0}},
        {11, "y", "Y", {110, 0}},
    })));
    EXPECT_TRUE(queue->push(create_log_entry_batch({
        {12, "z", "Z", {120, 0}},
    })));

    // End marker

    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    std::vector<std::tuple<storage_id_type, std::string, std::string>> expected = {
        {10, "x", "X"},
        {11, "y", "Y"},
        {12, "z", "Z"},
    };

    for (const auto& [expected_sid, expected_key, expected_value] : expected) {
        ASSERT_TRUE(cursor.next());
        EXPECT_EQ(cursor.storage(), expected_sid);

        std::string actual_key, actual_value;
        cursor.key(actual_key);
        cursor.value(actual_value);
        EXPECT_EQ(actual_key, expected_key);
        EXPECT_EQ(actual_value, expected_value);
    }

    EXPECT_FALSE(cursor.next());  // Reached end_marker
}

TEST_F(partitioned_cursor_impl_test, empty_batch_is_ignored) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor_impl cursor(queue);

    // Push an empty batch; expected to be ignored by the cursor
    std::vector<log_entry> empty_batch;
    EXPECT_TRUE(queue->push(empty_batch));  // push succeeds but should be ignored

    // Push a valid singleton batch
    EXPECT_TRUE(queue->push(create_singleton_log_entry_batch(99, "last", "entry", {9, 9})));

    // Push end marker to signal the end of stream
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    // Cursor should skip the empty batch and pick up the valid entry
    EXPECT_TRUE(cursor.next());
    EXPECT_EQ(cursor.storage(), 99);

    std::string key, value;
    cursor.key(key);
    cursor.value(value);
    EXPECT_EQ(key, "last");
    EXPECT_EQ(value, "entry");

    // Cursor should now reach the end
    EXPECT_FALSE(cursor.next());
}




}  // namespace limestone::testing
