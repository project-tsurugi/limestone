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
#include <stdexcept>
#include <vector>
#include <boost/filesystem.hpp>

#include "blob_file_gc_snapshot.h"
#include "blob_gc_entry.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry.h"

// Use the namespaces defined for the API and internal classes.
using namespace limestone::api;
using namespace limestone::internal;

namespace limestone {
namespace testing {

/*
 * Testable subclass to expose protected methods for unit testing.
 */
class testable_blob_file_gc_snapshot : public blob_file_gc_snapshot {
public:
    using blob_file_gc_snapshot::blob_file_gc_snapshot;  // inherit constructor

    // Expose protected methods as public.
    using blob_file_gc_snapshot::finalize_high_entries_impl;
    using blob_file_gc_snapshot::finalize_low_entries_impl;
};

// Test fixture for blob_file_gc_snapshot.
class blob_file_gc_snapshot_test : public ::testing::Test {
protected:
    /**
     * @brief Creates a normal_with_blob entry.
     * @param storage Storage ID.
     * @param key The key.
     * @param value The value.
     * @param wv The write_version.
     * @param blob_ids (Optional) vector of blob_id; default is empty.
     * @return log_entry The created log entry.
     */
    static log_entry create_blob_log_entry(storage_id_type storage,
                                           const std::string &key,
                                           const std::string &value,
                                           const write_version_type &wv,
                                           const std::vector<blob_id_type>& blob_ids = {}) {
        return log_entry::make_normal_with_blob(storage, key, value, wv, blob_ids);
    }

    // checks the key_sid (the storage_id and the key) and the write_version of a blob_gc_entry
    static void check_blob_gc_entry(const blob_gc_entry& entry,
                                    storage_id_type expected_storage,
                                    const std::string& expected_key,
                                    const write_version_type& expected_wv) {
        const std::string_view key_sid = entry.serialized_key_sid();
        EXPECT_EQ(log_entry_format::load_uint64le(key_sid), expected_storage);
        EXPECT_EQ(key_sid.substr(log_entry_format::storage_id_size), expected_key);
        EXPECT_EQ(entry.write_version(), expected_wv);
    }
};

/**
 * @brief Test that a valid blob entry is sanitized and added.
 *
 * The sanitize_and_add_entry() method should keep only the key_sid, the write_version and the blob_ids of the entry
 * (blob_gc_entry) and add it if its write_version is below the threshold.
 */
TEST_F(blob_file_gc_snapshot_test, sanitize_and_add_entry_valid) {
    storage_id_type storage = 100;
    std::string key = "testKey";
    std::string value = "testValue"; // This payload is not kept.
    write_version_type wv(50, 1);      // Entry write_version.
    
    // Create a blob log entry.
    log_entry entry = create_blob_log_entry(storage, key, value, wv, {7, 8});
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_with_blob);
    
    // Create a blob_file_gc_snapshot with a threshold higher than the entry's write_version.
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry.view());
    snapshot.finalize_local_entries();
    const blob_gc_entry_container& snap = snapshot.finalize_snapshot();
    
    // Expect one entry in the snapshot.
    EXPECT_EQ(snap.size(), 1u);
    
    // The added entry keeps storage, key, write_version and blob_ids.
    const blob_gc_entry& sanitized_entry = *snap.begin();
    check_blob_gc_entry(sanitized_entry, storage, key, wv);
    EXPECT_EQ(sanitized_entry.blob_ids(), (std::vector<blob_id_type>{7, 8}));
}

/**
 * @brief Test that a non-blob entry is ignored.
 *
 * Entries that are not of type normal_with_blob should not be added to the snapshot.
 */
TEST_F(blob_file_gc_snapshot_test, sanitize_and_add_entry_invalid_type) {
    // Create a normal entry (not a blob entry).
    storage_id_type storage = 200;
    std::string key = "normalKey";
    std::string value = "normalValue";
    write_version_type wv(50, 1);
    
    log_entry entry = log_entry::make_normal_entry(storage, key, value, wv);
    
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_entry);
    
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry.view());
    snapshot.finalize_local_entries();
    const blob_gc_entry_container& snap = snapshot.finalize_snapshot();
    
    // Since the entry is not normal_with_blob, it should not be added.
    EXPECT_EQ(snap.size(), 0u);
}

/**
 * @brief Test that reset() clears the snapshot.
 */
TEST_F(blob_file_gc_snapshot_test, reset_snapshot) {
    storage_id_type storage = 300;
    std::string key = "resetKey";
    std::string value = "resetValue";
    write_version_type wv(50, 1);
    
    log_entry entry = create_blob_log_entry(storage, key, value, wv);
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry.view());
    snapshot.finalize_local_entries();
    const blob_gc_entry_container& snap = snapshot.finalize_snapshot();
    EXPECT_EQ(snap.size(), 1u);
    
    // Reset the snapshot.
    snapshot.reset();
    
    // After reset, finalize_snapshot should return an empty snapshot.
    const blob_gc_entry_container& snap2 = snapshot.finalize_snapshot();
    EXPECT_EQ(snap2.size(), 0u);
}

/**
 * @brief Test that finalize_snapshot() merges duplicate entries.
 *
 * When multiple entries with the same key are added, finalize_snapshot() should remove duplicates,
 * keeping only the entry with the maximum write_version (since the container is sorted in descending order).
 */
TEST_F(blob_file_gc_snapshot_test, finalize_snapshot_merging_duplicates) {
    storage_id_type storage = 400;
    std::string key = "dupKey";
    std::string value1 = "val1";
    std::string value2 = "val2";
    write_version_type wv_low(10, 1);  // Lower version.
    write_version_type wv_high(10, 2); // Higher version.
    
    // Create two blob entries with the same key.
    log_entry entry1 = create_blob_log_entry(storage, key, value1, wv_low);
    log_entry entry2 = create_blob_log_entry(storage, key, value2, wv_high);
    
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry1.view());
    snapshot.sanitize_and_add_entry(entry2.view());
    snapshot.finalize_local_entries();
    const blob_gc_entry_container& snap = snapshot.finalize_snapshot();
    
    // After merging and duplicate removal, only one entry should remain.
    EXPECT_EQ(snap.size(), 1u);
    
    // The remaining entry should have the maximum write_version (wv_high).
    const blob_gc_entry& merged_entry = *snap.begin();
    const write_version_type merged_wv = merged_entry.write_version();
    EXPECT_EQ(merged_wv.get_major(), wv_high.get_major());
    EXPECT_EQ(merged_wv.get_minor(), wv_high.get_minor());
}

TEST_F(blob_file_gc_snapshot_test, tls_container_null_state_behavior) {
    // Create a snapshot instance with a given threshold.
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    EXPECT_EQ(snapshot.boundary_version(), write_version_type(100, 1));

    // Ensure the internal state is reset so that tls_container_ becomes nullptr.
    snapshot.reset();

    // Calling finalize_local_entries() when tls_container_ is nullptr should not crash.
    EXPECT_NO_THROW(snapshot.finalize_local_entries());
    const blob_gc_entry_container& snap1 = snapshot.finalize_snapshot();
    EXPECT_EQ(snap1.size(), 0u);

    // Now, create a valid blob entry.
    storage_id_type storage = 500;
    std::string key = "boundaryKey";
    std::string value = "boundaryValue";
    write_version_type wv(50, 1);
    log_entry entry = create_blob_log_entry(storage, key, value, wv);

    // Calling sanitize_and_add_entry() when tls_container_ is nullptr should not crash
    // and should create a new container to add the entry.
    EXPECT_NO_THROW(snapshot.sanitize_and_add_entry(entry.view()));
    snapshot.finalize_local_entries();
    const blob_gc_entry_container& snap2 = snapshot.finalize_snapshot();
    EXPECT_EQ(snap2.size(), 1u);
}

TEST_F(blob_file_gc_snapshot_test, threshold_boundary_test) {
    // Create a snapshot with threshold write_version (100, 1)
    testable_blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    EXPECT_EQ(snapshot.boundary_version(), write_version_type(100, 1));

    // Case 1: Entry with write_version exactly equal to threshold.
    // Expected: The entry should NOT be added (because it's not less than the threshold).
    log_entry entry_equal = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry_equal.view());
    snapshot.finalize_local_entries();
    blob_gc_entry_container snap_equal = snapshot.finalize_low_entries_impl();
    EXPECT_EQ(snap_equal.size(), 0u);
    snap_equal = snapshot.finalize_high_entries_impl();
    EXPECT_EQ(snap_equal.size(), 1u);

    // Reset snapshot state.
    snapshot.reset();

    // Case 2: Entry with write_version just below the threshold.
    // For example, (100, 0) is less than (100, 1); expected: the entry should be added.
    log_entry entry_lower = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(100, 0));
    snapshot.sanitize_and_add_entry(entry_lower.view());
    snapshot.finalize_local_entries();
    blob_gc_entry_container snap_lower = snapshot.finalize_low_entries_impl();
    EXPECT_EQ(snap_lower.size(), 1u);
    snap_lower = snapshot.finalize_high_entries_impl();
    EXPECT_EQ(snap_lower.size(), 1u);

    // Reset snapshot state.
    snapshot.reset();

    // Case 3: Entry with write_version above the threshold.
    // For example, (101, 0) is greater than (100, 1); expected: the entry should NOT be added.
    log_entry entry_higher = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(101, 0));
    snapshot.sanitize_and_add_entry(entry_higher.view());
    snapshot.finalize_local_entries();
    blob_gc_entry_container snap_higher = snapshot.finalize_low_entries_impl();
    EXPECT_EQ(snap_higher.size(), 0u);
    snap_higher = snapshot.finalize_high_entries_impl();
    EXPECT_EQ(snap_higher.size(), 1u);
}



} // namespace testing
} // namespace limestone
