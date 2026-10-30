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

} // namespace limestone::api
