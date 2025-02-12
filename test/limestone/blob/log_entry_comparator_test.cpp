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
using namespace limestone::api;         // For log_entry, storage_id_type, write_version_type, epoch_id_type, etc.
using namespace limestone::internal;    // For log_entry_comparator

namespace limestone {
namespace testing {

// Test fixture renamed to avoid symbol collisions with datastore_blob_test.
class log_entry_comparator_unit_test : public ::testing::Test {
protected:
    std::string temp_dir;
    int file_counter = 0;

    // Set up a temporary directory named "/tmp/limestone_log_entry_comparator_test"
    // after ensuring its parent directory is writable.
    void SetUp() override {
        // Change permissions and remove any previous test directory.
        if (system("chmod -R a+rwx /tmp") != 0) {
            std::cerr << "cannot change permission" << std::endl;
        }
        if (system("rm -rf /tmp/limestone_log_entry_comparator_test") != 0) {
            std::cerr << "cannot remove directory /tmp/limestone_log_entry_comparator_test" << std::endl;
        }
        if (system("mkdir -p /tmp/limestone_log_entry_comparator_test") != 0) {
            std::cerr << "cannot make directory /tmp/limestone_log_entry_comparator_test" << std::endl;
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

// -----------------------------------------------------------------------------
// New comparator specification:
//
// 1. First, compare key_sid() as a binary string (the entire string as stored).
//    - If a.key_sid() != b.key_sid(), then the one with the lexicographically smaller
//      key_sid() comes first.
// 2. If key_sid() values are completely equal, then compare write_version in descending order.
//    (That is, the entry with the higher write_version comes first.)
// -----------------------------------------------------------------------------

// Test case 1: Different write_versions but identical keys.
// Since keys are equal, ordering is determined by write_version in descending order.
TEST_F(log_entry_comparator_unit_test, different_write_versions) {
    // Use the same key and value for both entries.
    storage_id_type storage = 100;
    std::string key = "testKey";
    std::string value = "testValue";

    // Create write_version_type instances via the public constructor.
    write_version_type wv1(10, 1); // Higher version
    write_version_type wv2(5, 1);  // Lower version

    // Both entries have the same key_sid.
    log_entry entry1 = create_normal_log_entry(storage, key, value, wv1);
    log_entry entry2 = create_normal_log_entry(storage, key, value, wv2);

    log_entry_comparator comp;
    // Since keys are equal, ordering is determined by write_version in descending order.
    EXPECT_TRUE(comp(entry1, entry2));
    EXPECT_FALSE(comp(entry2, entry1));
}

// Test case 2: Equal write_versions but different keys.
// Ordering is solely determined by key_sid() using lexicographical (binary) ascending order.
TEST_F(log_entry_comparator_unit_test, equal_write_versions_different_keys) {
    storage_id_type storage = 200;
    // Supply different user key portions.
    std::string key1 = "zzz";  // This will be part of key_sid.
    std::string key2 = "aaa";  // This will be part of key_sid.
    std::string value = "testValue";
    write_version_type wv(7, 3);

    log_entry entry1 = create_normal_log_entry(storage, key1, value, wv);
    log_entry entry2 = create_normal_log_entry(storage, key2, value, wv);

    log_entry_comparator comp;
    // Since keys are different and write_version is equal,
    // the one with the lexicographically smaller key_sid() comes first.
    // Assuming that log_entry::write() stores the key as (storage_id + key),
    // and that the binary comparison reflects the user key,
    // "aaa" < "zzz" so entry2 should come before entry1.
    EXPECT_TRUE(comp(entry2, entry1));
    EXPECT_FALSE(comp(entry1, entry2));
}

// Test case 3: Equal write_versions and equal keys.
// In this case, both key_sid() and write_version are identical,
// so the comparator should return false for both orderings.
TEST_F(log_entry_comparator_unit_test, equal_write_versions_equal_keys) {
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

// Test case 4: Mixed comparison.
// Create three entries with differing keys and write_versions.
// Since key_sid() is the primary comparison, if keys differ the ordering is determined solely by key_sid().
TEST_F(log_entry_comparator_unit_test, mixed_comparison) {
    // Create three entries:
    //   entry1: write_version = (8, 0), key = "bbb"
    //   entry2: write_version = (8, 0), key = "ccc"
    //   entry3: write_version = (10, 0), key = "aaa"
    storage_id_type storage = 400;
    std::string value = "testValue";
    write_version_type wv1(8, 0);
    write_version_type wv2(8, 0);
    write_version_type wv3(10, 0);

    log_entry entry1 = create_normal_log_entry(storage, "bbb", value, wv1);
    log_entry entry2 = create_normal_log_entry(storage, "ccc", value, wv2);
    log_entry entry3 = create_normal_log_entry(storage, "aaa", value, wv3);

    log_entry_comparator comp;
    // Since key_sid() is primary:
    // Lexicographically (binary), assuming the stored key reflects the user key,
    // "aaa" < "bbb" < "ccc". Therefore, entry3 (with key "aaa") should come first,
    // then entry1 ("bbb"), then entry2 ("ccc").
    EXPECT_TRUE(comp(entry3, entry1));
    EXPECT_TRUE(comp(entry3, entry2));
    EXPECT_TRUE(comp(entry1, entry2));

    EXPECT_FALSE(comp(entry1, entry3));
    EXPECT_FALSE(comp(entry2, entry3));
    EXPECT_FALSE(comp(entry2, entry1));
}

} // namespace testing
} // namespace limestone
