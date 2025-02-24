#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <boost/filesystem.hpp>

// Include the actual headers for log_entry and log_entry_comparator.
#include "log_entry.h"
#include "log_entry_comparator.h"

// Use the appropriate namespaces for the API and internal classes.
using namespace limestone::api;      // For log_entry, storage_id_type, write_version_type, epoch_id_type, etc.
using namespace limestone::internal; // For log_entry_comparator

namespace limestone {
namespace testing {

// Test fixture to avoid symbol collisions with datastore_blob_test.
class log_entry_comparator_test : public ::testing::Test {
protected:
    std::string temp_dir;
    int file_counter = 0;

    // Set up a temporary directory "/tmp/limestone_log_entry_comparator_test"
    // after ensuring that its parent directory is writable.
    void SetUp() override {
        // Remove any previous test directory.
        if (system("rm -rf /tmp/limestone_log_entry_comparator_test") != 0) {
            std::cerr << "Cannot remove directory /tmp/limestone_log_entry_comparator_test" << std::endl;
        }
        // Create the test directory.
        if (system("mkdir -p /tmp/limestone_log_entry_comparator_test") != 0) {
            std::cerr << "Cannot create directory /tmp/limestone_log_entry_comparator_test" << std::endl;
        }
        temp_dir = "/tmp/limestone_log_entry_comparator_test";
        file_counter = 0;
    }

    // Tear down: remove the temporary directory.
    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }

    // Helper function: generate a unique temporary file name within temp_dir.
    std::string get_temp_file_name() {
        file_counter++;
        return temp_dir + "/temp_file_" + std::to_string(file_counter);
    }

    // Helper function: create a log_entry instance by writing a normal entry
    // into a temporary file and then reading it back.
    log_entry create_normal_log_entry(storage_id_type storage,
                                      const std::string &key,
                                      const std::string &value,
                                      const write_version_type &wversion) {
        std::string temp_file = get_temp_file_name();
        FILE* out = std::fopen(temp_file.c_str(), "wb");
        if (!out) {
            throw std::runtime_error("Failed to open temporary file for writing.");
        }
        // Write a normal entry using log_entry::write()
        log_entry::write(out, storage, key, value, wversion);
        std::fclose(out);

        std::ifstream in(temp_file, std::ios::binary);
        if (!in.is_open()) {
            throw std::runtime_error("Failed to open temporary file for reading.");
        }
        log_entry entry;
        bool rc = entry.read(in);
        in.close();
        boost::filesystem::remove(temp_file);
        if (!rc) {
            throw std::runtime_error("Failed to read log entry from temporary file.");
        }
        return entry;
    }
};

// Test case: Different minor numbers with the same epoch.
TEST_F(log_entry_comparator_test, different_minor_numbers) {
    storage_id_type storage = 400;
    std::string key = "testKey";
    std::string value = "testValue";
    
    // Both entries have the same epoch (10) but different minor numbers.
    // In ascending order, write_version (10,1) is less than (10,2).
    write_version_type wv_low(10, 1);  // Lower minor number
    write_version_type wv_high(10, 2); // Higher minor number

    log_entry entry_low = create_normal_log_entry(storage, key, value, wv_low);
    log_entry entry_high = create_normal_log_entry(storage, key, value, wv_high);

    log_entry_comparator comp;
    // Since keys are equal, ordering is determined by write_version in ascending order.
    EXPECT_TRUE(comp(entry_low, entry_high));
    EXPECT_FALSE(comp(entry_high, entry_low));
}

// Test case: Different epochs with the same minor number.
TEST_F(log_entry_comparator_test, different_epoch_numbers) {
    storage_id_type storage = 500;
    std::string key = "testKey";
    std::string value = "testValue";
    
    // Both entries have the same minor number (1) but different epochs.
    // In ascending order, write_version (10,1) is less than (20,1).
    write_version_type wv_low(10, 1);  // Lower epoch
    write_version_type wv_high(20, 1); // Higher epoch

    log_entry entry_low = create_normal_log_entry(storage, key, value, wv_low);
    log_entry entry_high = create_normal_log_entry(storage, key, value, wv_high);

    log_entry_comparator comp;
    // Since keys are equal, ordering is determined by write_version in ascending order.
    EXPECT_TRUE(comp(entry_low, entry_high));
    EXPECT_FALSE(comp(entry_high, entry_low));
}

// Test case: Equal epochs and minor numbers.
TEST_F(log_entry_comparator_test, equal_epoch_and_minor) {
    storage_id_type storage = 600;
    std::string key = "testKey";
    std::string value = "testValue";
    
    // Both entries have identical write_versions.
    write_version_type wv(15, 3);

    log_entry entry1 = create_normal_log_entry(storage, key, value, wv);
    log_entry entry2 = create_normal_log_entry(storage, key, value, wv);

    log_entry_comparator comp;
    // When write_version and keys are equal, neither entry is considered less than the other.
    EXPECT_FALSE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case: Verify that epoch is prioritized over minor in write_version comparison.
TEST_F(log_entry_comparator_test, epoch_priority_over_minor) {
    storage_id_type storage = 700;
    std::string key = "testKey";
    std::string value = "testValue";
    
    // Create write_versions:
    // One entry with (epoch=10, minor=100) and another with (epoch=11, minor=1).
    // Since epoch is prioritized, (10,100) is less than (11,1) in ascending order.
    write_version_type wv_low_epoch(10, 100);
    write_version_type wv_high_epoch(11, 1);

    log_entry entry_low_epoch = create_normal_log_entry(storage, key, value, wv_low_epoch);
    log_entry entry_high_epoch = create_normal_log_entry(storage, key, value, wv_high_epoch);

    log_entry_comparator comp;
    EXPECT_TRUE(comp(entry_low_epoch, entry_high_epoch));
    EXPECT_FALSE(comp(entry_high_epoch, entry_low_epoch));
}

// Test case: Equal write_versions but different keys.
// Ordering is determined solely by key_sid() using lexicographical order (ascending).
TEST_F(log_entry_comparator_test, equal_write_versions_different_keys) {
    storage_id_type storage = 200;
    // Use different user keys.
    std::string key1 = "zzz";  // Part of key_sid.
    std::string key2 = "aaa";  // Part of key_sid.
    std::string value = "testValue";
    write_version_type wv(7, 3);

    log_entry entry1 = create_normal_log_entry(storage, key1, value, wv);
    log_entry entry2 = create_normal_log_entry(storage, key2, value, wv);

    log_entry_comparator comp;
    // Assuming key_sid() includes storage and key, "aaa" is lexicographically less than "zzz".
    EXPECT_TRUE(comp(entry2, entry1));
    EXPECT_FALSE(comp(entry1, entry2));
}

// Test case: Equal write_versions and equal keys.
// When both key_sid() and write_version are identical, neither entry is less than the other.
TEST_F(log_entry_comparator_test, equal_write_versions_equal_keys) {
    storage_id_type storage = 300;
    std::string key = "sameKey";
    std::string value = "testValue";
    write_version_type wv(8, 2);

    log_entry entry1 = create_normal_log_entry(storage, key, value, wv);
    log_entry entry2 = create_normal_log_entry(storage, key, value, wv);

    log_entry_comparator comp;
    EXPECT_FALSE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case: Compare entries with different storage IDs.
// Assumes that key_sid() includes the storage ID and that a lower storage ID produces a lexicographically smaller key_sid().
TEST_F(log_entry_comparator_test, different_storage_ids) {
    // Create two log_entry objects with the same key and write_version, but different storage IDs.
    std::string key = "testKey";
    std::string value = "testValue";
    write_version_type wv(10, 1);

    // Define two different storage IDs.
    storage_id_type storage1 = 100;
    storage_id_type storage2 = 200;

    log_entry entry1 = create_normal_log_entry(storage1, key, value, wv);
    log_entry entry2 = create_normal_log_entry(storage2, key, value, wv);

    log_entry_comparator comp;
    // Expect that the entry with the lower storage ID produces a key_sid that is lexicographically smaller.
    EXPECT_TRUE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case: Overall priority test of storage ID, key, and write_version in log_entry comparison.
// Priority order (all in ascending order):
// 1. Storage ID (lower is prioritized)
// 2. Key (lexicographical order when storage IDs are equal)
// 3. Write Version (compared by epoch first, then minor when storage and key are equal)
TEST_F(log_entry_comparator_test, overall_priority_storage_key_write_version) {
    // Create four log_entry objects with various combinations:
    //
    // Entry1: storage=100, key="aaa", write_version=(10,0)
    // Entry2: storage=100, key="aaa", write_version=(10,1)   (Same storage and key as Entry1, but higher write_version)
    // Entry3: storage=100, key="bbb", write_version=(9,9)    (Same storage as Entry1/2, but key "bbb")
    // Entry4: storage=200, key="aaa", write_version=(9,9)    (Higher storage than Entry1-3; key is "aaa")
    
    std::string value = "testValue"; // Value is not used for sorting.
    storage_id_type storage_low = 100;
    storage_id_type storage_high = 200;
    
    // Entries with same storage and key, differing only in write_version.
    write_version_type wv1(10, 0);
    write_version_type wv2(10, 1);
    // Entry3: same storage as Entry1/2, but with key "bbb".
    write_version_type wv3(9, 9);
    // Entry4: different storage, key "aaa".
    write_version_type wv4(9, 9);
    
    log_entry entry1 = create_normal_log_entry(storage_low, "aaa", value, wv1);
    log_entry entry2 = create_normal_log_entry(storage_low, "aaa", value, wv2);
    log_entry entry3 = create_normal_log_entry(storage_low, "bbb", value, wv3);
    log_entry entry4 = create_normal_log_entry(storage_high, "aaa", value, wv4);
    
    // Place entries in unsorted order.
    std::vector<log_entry> entries = { entry3, entry4, entry2, entry1 };
    
    // Sort using log_entry_comparator.
    log_entry_comparator comp;
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
    auto describe = [](const log_entry& entry) -> std::string {
        std::ostringstream oss;
        oss << "key_sid: " << entry.key_sid();
        write_version_type wv;
        entry.write_version(wv);
        oss << ", write_version: (" << wv.get_major() << "," << wv.get_minor() << ")";
        return oss.str();
    };
    
    for (size_t i = 0; i < entries.size(); ++i) {
        std::cout << "Entry " << i+1 << ": " << describe(entries[i]) << std::endl;
    }
}

} // namespace testing
} // namespace limestone
