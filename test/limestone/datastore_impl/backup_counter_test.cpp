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
#include <chrono>
#include <future>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include "blob_file_garbage_collector.h"
#include "blob_file_resolver.h"
#include "datastore_impl.h"

namespace limestone::api {

class backup_counter_test : public ::testing::Test {
protected:
    backup_counter_test() = default;
    ~backup_counter_test() override = default;
};

TEST_F(backup_counter_test, backup_counter_increments) {
    datastore_impl datastore;
    
    // Increment the counter
    datastore.increment_backup_counter();
    
    // Check that backup is in progress
    EXPECT_TRUE(datastore.is_backup_in_progress());
    
    // Decrement the counter
    datastore.decrement_backup_counter();
    
    // Check that backup is no longer in progress
    EXPECT_FALSE(datastore.is_backup_in_progress());
}

TEST_F(backup_counter_test, backup_counter_multiple_increments) {
    datastore_impl datastore;
    
    // Increment multiple times
    for (int i = 0; i < 5; ++i) {
        datastore.increment_backup_counter();
    }
    
    // Backup should be in progress
    EXPECT_TRUE(datastore.is_backup_in_progress());
    
    // Decrement back to 0
    for (int i = 0; i < 5; ++i) {
        datastore.decrement_backup_counter();
    }
    
    // Backup should no longer be in progress
    EXPECT_FALSE(datastore.is_backup_in_progress());
}

TEST_F(backup_counter_test, backup_counter_does_not_go_negative) {
    datastore_impl datastore;
    
    // Decrement without incrementing
    datastore.decrement_backup_counter();
    
    // Ensure that backup is not in progress
    EXPECT_FALSE(datastore.is_backup_in_progress());
}

// The start of a backup stops a running BLOB file GC, and makes a backup in progress
TEST_F(backup_counter_test, start_backup_exclusion_stops_running_blob_gc) {
    const boost::filesystem::path location{"/tmp/backup_counter_test"};
    boost::filesystem::remove_all(location);
    limestone::internal::blob_file_resolver resolver(location);
    ASSERT_TRUE(boost::filesystem::create_directories(resolver.get_blob_root()));
    limestone::internal::blob_file_garbage_collector gc(resolver);
    // Start only the scan. The GC stays running, waiting for the list of the BLOBs exempt from the removal
    gc.scan_blob_files(1);
    ASSERT_TRUE(gc.is_active());
    datastore_impl datastore;

    datastore.start_backup_exclusion(&gc);

    EXPECT_FALSE(gc.is_active());
    EXPECT_TRUE(datastore.is_backup_in_progress());
    boost::filesystem::remove_all(location);
}

// It makes a backup in progress even without a garbage collector
TEST_F(backup_counter_test, start_backup_exclusion_without_blob_gc) {
    datastore_impl datastore;

    datastore.start_backup_exclusion(nullptr);

    EXPECT_TRUE(datastore.is_backup_in_progress());
}

// A compaction does not begin during a backup. The start of a backup waits for a compaction to complete
TEST_F(backup_counter_test, compaction_and_backup_exclude_each_other) {
    datastore_impl datastore;

    datastore.increment_backup_counter();
    EXPECT_FALSE(datastore.try_begin_compaction());
    datastore.decrement_backup_counter();

    ASSERT_TRUE(datastore.try_begin_compaction());
    std::future<void> backup = std::async(std::launch::async, [&datastore]() { datastore.increment_backup_counter(); });
    EXPECT_EQ(backup.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);
    EXPECT_FALSE(datastore.is_backup_in_progress());

    datastore.end_compaction();
    backup.get();
    EXPECT_TRUE(datastore.is_backup_in_progress());
}

} // namespace limestone::api
