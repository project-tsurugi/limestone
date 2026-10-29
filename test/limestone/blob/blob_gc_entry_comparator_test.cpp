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
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <stdexcept>
#include <vector>
#include <boost/filesystem.hpp>

// Include the actual headers for log_entry and blob_gc_entry_comparator.
#include "log_entry.h"
#include "blob_gc_entry.h"
#include "log_entry/log_entry_format.h"
#include "blob_gc_entry_comparator.h"

// Use the appropriate namespaces for the API and internal classes.
using namespace limestone::api;      // For log_entry, storage_id_type, write_version_type, epoch_id_type, etc.
using namespace limestone::internal; // For blob_gc_entry_comparator

namespace limestone::testing {

// Test fixture to avoid symbol collisions with datastore_blob_test.
class blob_gc_entry_comparator_test : public ::testing::Test {
protected:
    // creates a blob_gc_entry from a view of a normal_with_blob
    static blob_gc_entry create_blob_gc_entry(storage_id_type storage,
                                              const std::string &key,
                                              const write_version_type &wversion) {
        return blob_gc_entry{log_entry::make_normal_with_blob(storage, key, "", wversion, {}).view()};
    }

    // take the storage_id and the key out of the key_sid of a blob_gc_entry
    static storage_id_type storage_of(const blob_gc_entry& entry) {
        return static_cast<storage_id_type>(log_entry_format::load_uint64le(entry.serialized_key_sid()));
    }
    static std::string key_of(const blob_gc_entry& entry) {
        return std::string{entry.serialized_key_sid().substr(log_entry_format::storage_id_size)};
    }
};

// Test case: Different minor numbers with the same epoch.
TEST_F(blob_gc_entry_comparator_test, different_minor_numbers) {
    storage_id_type storage = 400;
    std::string key = "testKey";

    // Both entries have the same epoch (10) but different minor numbers.
    // In ascending order, write_version (10,1) is less than (10,2).
    write_version_type wv_low(10, 1);  // Lower minor number
    write_version_type wv_high(10, 2); // Higher minor number

    blob_gc_entry entry_low = create_blob_gc_entry(storage, key, wv_low);
    blob_gc_entry entry_high = create_blob_gc_entry(storage, key, wv_high);

    blob_gc_entry_comparator comp;
    // Since keys are equal, ordering is determined by write_version in ascending order.
    EXPECT_TRUE(comp(entry_low, entry_high));
    EXPECT_FALSE(comp(entry_high, entry_low));
}

// Test case: Different epochs with the same minor number.
TEST_F(blob_gc_entry_comparator_test, different_epoch_numbers) {
    storage_id_type storage = 500;
    std::string key = "testKey";

    // Both entries have the same minor number (1) but different epochs.
    // In ascending order, write_version (10,1) is less than (20,1).
    write_version_type wv_low(10, 1);  // Lower epoch
    write_version_type wv_high(20, 1); // Higher epoch

    blob_gc_entry entry_low = create_blob_gc_entry(storage, key, wv_low);
    blob_gc_entry entry_high = create_blob_gc_entry(storage, key, wv_high);

    blob_gc_entry_comparator comp;
    // Since keys are equal, ordering is determined by write_version in ascending order.
    EXPECT_TRUE(comp(entry_low, entry_high));
    EXPECT_FALSE(comp(entry_high, entry_low));
}

// Test case: Equal epochs and minor numbers.
TEST_F(blob_gc_entry_comparator_test, equal_epoch_and_minor) {
    storage_id_type storage = 600;
    std::string key = "testKey";

    // Both entries have identical write_versions.
    write_version_type wv(15, 3);

    blob_gc_entry entry1 = create_blob_gc_entry(storage, key, wv);
    blob_gc_entry entry2 = create_blob_gc_entry(storage, key, wv);

    blob_gc_entry_comparator comp;
    // When write_version and keys are equal, neither entry is considered less than the other.
    EXPECT_FALSE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case: Verify that epoch is prioritized over minor in write_version comparison.
TEST_F(blob_gc_entry_comparator_test, epoch_priority_over_minor) {
    storage_id_type storage = 700;
    std::string key = "testKey";

    // Create write_versions:
    // One entry with (epoch=10, minor=100) and another with (epoch=11, minor=1).
    // Since epoch is prioritized, (10,100) is less than (11,1) in ascending order.
    write_version_type wv_low_epoch(10, 100);
    write_version_type wv_high_epoch(11, 1);

    blob_gc_entry entry_low_epoch = create_blob_gc_entry(storage, key, wv_low_epoch);
    blob_gc_entry entry_high_epoch = create_blob_gc_entry(storage, key, wv_high_epoch);

    blob_gc_entry_comparator comp;
    EXPECT_TRUE(comp(entry_low_epoch, entry_high_epoch));
    EXPECT_FALSE(comp(entry_high_epoch, entry_low_epoch));
}

// Test case: Equal write_versions but different keys.
// Ordering is determined solely by key_sid() using lexicographical order (ascending).
TEST_F(blob_gc_entry_comparator_test, equal_write_versions_different_keys) {
    storage_id_type storage = 200;
    // Use different user keys.
    std::string key1 = "zzz";  // Part of key_sid.
    std::string key2 = "aaa";  // Part of key_sid.
    write_version_type wv(7, 3);

    blob_gc_entry entry1 = create_blob_gc_entry(storage, key1, wv);
    blob_gc_entry entry2 = create_blob_gc_entry(storage, key2, wv);

    blob_gc_entry_comparator comp;
    // Assuming key_sid() includes storage and key, "aaa" is lexicographically less than "zzz".
    EXPECT_TRUE(comp(entry2, entry1));
    EXPECT_FALSE(comp(entry1, entry2));
}

// Test case: Equal write_versions and equal keys.
// When both key_sid() and write_version are identical, neither entry is less than the other.
TEST_F(blob_gc_entry_comparator_test, equal_write_versions_equal_keys) {
    storage_id_type storage = 300;
    std::string key = "sameKey";
    write_version_type wv(8, 2);

    blob_gc_entry entry1 = create_blob_gc_entry(storage, key, wv);
    blob_gc_entry entry2 = create_blob_gc_entry(storage, key, wv);

    blob_gc_entry_comparator comp;
    EXPECT_FALSE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case: Compare entries with different storage IDs.
// key_sid() starts with the little-endian storage ID, and the order is the lexicographical order of the key_sid() bytes.
// For storage IDs that fit in one byte, this order matches the numeric order of the storage IDs.
TEST_F(blob_gc_entry_comparator_test, different_storage_ids) {
    // Create two log_entry objects with the same key and write_version, but different storage IDs.
    std::string key = "testKey";
    write_version_type wv(10, 1);

    // Define two different storage IDs.
    storage_id_type storage1 = 100;
    storage_id_type storage2 = 200;

    blob_gc_entry entry1 = create_blob_gc_entry(storage1, key, wv);
    blob_gc_entry entry2 = create_blob_gc_entry(storage2, key, wv);

    blob_gc_entry_comparator comp;
    // The storage IDs fit in one byte, so the numerically smaller one comes first.
    EXPECT_TRUE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// For storage IDs of two or more bytes, the lexicographical order of the key_sid() bytes does not match the numeric order.
// The first bytes are reversed: 256 (00 01 ...) comes before 1 (01 00 ...).
TEST_F(blob_gc_entry_comparator_test, multibyte_storage_ids_first_byte_reversed) {
    write_version_type wv(10, 1);
    blob_gc_entry entry_256 = create_blob_gc_entry(256, "testKey", wv);
    blob_gc_entry entry_1 = create_blob_gc_entry(1, "testKey", wv);

    blob_gc_entry_comparator comp;
    EXPECT_TRUE(comp(entry_256, entry_1));
    EXPECT_FALSE(comp(entry_1, entry_256));
}

// Both are two bytes: 513 (01 02 ...) comes before 258 (02 01 ...).
TEST_F(blob_gc_entry_comparator_test, multibyte_storage_ids_both_two_bytes) {
    write_version_type wv(10, 1);
    blob_gc_entry entry_513 = create_blob_gc_entry(513, "testKey", wv);
    blob_gc_entry entry_258 = create_blob_gc_entry(258, "testKey", wv);

    blob_gc_entry_comparator comp;
    EXPECT_TRUE(comp(entry_513, entry_258));
    EXPECT_FALSE(comp(entry_258, entry_513));
}

// The first bytes are equal, so the second byte decides: 65537 (01 00 01 ...) comes before 257 (01 01 00 ...).
TEST_F(blob_gc_entry_comparator_test, multibyte_storage_ids_same_first_byte) {
    write_version_type wv(10, 1);
    blob_gc_entry entry_65537 = create_blob_gc_entry(65537, "testKey", wv);
    blob_gc_entry entry_257 = create_blob_gc_entry(257, "testKey", wv);

    blob_gc_entry_comparator comp;
    EXPECT_TRUE(comp(entry_65537, entry_257));
    EXPECT_FALSE(comp(entry_257, entry_65537));
}

// Sorts entries including storage IDs of two or more bytes, and checks that they are ordered by the key_sid() bytes
// (starting with the little-endian storage ID), then key, then write_version. The expected order is written by hand,
// independently of the comparator.
TEST_F(blob_gc_entry_comparator_test, multibyte_storage_ids_overall_sort) {
    struct spec {
        storage_id_type storage;
        std::string key;
        write_version_type wv;
    };
    // The expected order. The first two bytes of the storage IDs are 256: 00 01, 1: 01 00, 257: 01 01, 513: 01 02, 258: 02 01.
    std::vector<spec> expected = {
        {256, "aaa", write_version_type(5, 0)},
        {256, "bbb", write_version_type(1, 0)},
        {1, "aaa", write_version_type(3, 0)},
        {1, "aaa", write_version_type(3, 1)},
        {1, "aaa", write_version_type(4, 0)},
        {257, "aaa", write_version_type(1, 0)},
        {513, "aaa", write_version_type(1, 0)},
        {258, "aaa", write_version_type(1, 0)},
    };

    std::vector<blob_gc_entry> entries;
    entries.reserve(expected.size());
    for (auto it = expected.rbegin(); it != expected.rend(); ++it) {
        entries.emplace_back(create_blob_gc_entry(it->storage, it->key, it->wv));
    }

    std::sort(entries.begin(), entries.end(), blob_gc_entry_comparator{});

    ASSERT_EQ(entries.size(), expected.size());
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const std::string key = key_of(entries[i]);
        const write_version_type wv = entries[i].write_version();
        EXPECT_EQ(storage_of(entries[i]), expected[i].storage) << "index " << i;
        EXPECT_EQ(key, expected[i].key) << "index " << i;
        EXPECT_EQ(wv.get_major(), expected[i].wv.get_major()) << "index " << i;
        EXPECT_EQ(wv.get_minor(), expected[i].wv.get_minor()) << "index " << i;
    }
}

// Test case: Overall priority test of storage ID, key, and write_version in log_entry comparison.
// Priority order (all in ascending order):
// 1. key_sid() bytes (starting with the little-endian storage ID). The storage IDs in this test fit in one byte,
//    so the order matches the numeric order.
// 2. Key (lexicographical order when storage IDs are equal)
// 3. Write Version (compared by epoch first, then minor when storage and key are equal)
TEST_F(blob_gc_entry_comparator_test, overall_priority_storage_key_write_version) {
    // Create four log_entry objects with various combinations:
    //
    // Entry1: storage=100, key="aaa", write_version=(10,0)
    // Entry2: storage=100, key="aaa", write_version=(10,1)   (Same storage and key as Entry1, but higher write_version)
    // Entry3: storage=100, key="bbb", write_version=(9,9)    (Same storage as Entry1/2, but key "bbb")
    // Entry4: storage=200, key="aaa", write_version=(9,9)    (Higher storage than Entry1-3; key is "aaa")

    storage_id_type storage_low = 100;
    storage_id_type storage_high = 200;

    // Entries with same storage and key, differing only in write_version.
    write_version_type wv1(10, 0);
    write_version_type wv2(10, 1);
    // Entry3: same storage as Entry1/2, but with key "bbb".
    write_version_type wv3(9, 9);
    // Entry4: different storage, key "aaa".
    write_version_type wv4(9, 9);

    blob_gc_entry entry1 = create_blob_gc_entry(storage_low, "aaa", wv1);
    blob_gc_entry entry2 = create_blob_gc_entry(storage_low, "aaa", wv2);
    blob_gc_entry entry3 = create_blob_gc_entry(storage_low, "bbb", wv3);
    blob_gc_entry entry4 = create_blob_gc_entry(storage_high, "aaa", wv4);

    // Place entries in unsorted order.
    std::vector<blob_gc_entry> entries = { entry3, entry4, entry2, entry1 };

    // Sort using blob_gc_entry_comparator.
    blob_gc_entry_comparator comp;
    std::sort(entries.begin(), entries.end(), comp);

    // Expected sorted order (ascending):
    // 1. entry1: storage=100, key="aaa", write_version=(10,0)
    // 2. entry2: storage=100, key="aaa", write_version=(10,1)
    // 3. entry3: storage=100, key="bbb", write_version=(9,9)
    // 4. entry4: storage=200, key="aaa", write_version=(9,9)
    //
    // Explanation:
    // - Entries with storage=100 come before those with storage=200.
    // - Among entries with storage=100 and key "aaa", (10,0) < (10,1).
    // - For the same storage, "aaa" is lexicographically less than "bbb".

    // Verify the sorted order using the comparator.
    EXPECT_TRUE(comp(entries[0], entries[1]));  // entry1 < entry2
    EXPECT_TRUE(comp(entries[1], entries[2]));  // entry2 < entry3
    EXPECT_TRUE(comp(entries[2], entries[3]));  // entry3 < entry4

    // Optionally, print the sorted order for manual verification.
    auto describe = [](const blob_gc_entry& entry) -> std::string {
        std::ostringstream oss;
        oss << "key_sid: " << entry.serialized_key_sid();
        const write_version_type wv = entry.write_version();
        oss << ", write_version: (" << wv.get_major() << "," << wv.get_minor() << ")";
        return oss.str();
    };

    for (size_t i = 0; i < entries.size(); ++i) {
        std::cout << "Entry " << i+1 << ": " << describe(entries[i]) << std::endl;
    }
}

}  // namespace limestone::testing
