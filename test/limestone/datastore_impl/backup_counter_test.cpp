#include <gtest/gtest.h>
#include "datastore_impl.h"

namespace limestone::testing {

using namespace limestone::api;

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
