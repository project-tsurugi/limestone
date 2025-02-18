#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <vector>
#include <boost/filesystem.hpp>

#include "blob_file_gc_snapshot.h"
#include "log_entry.h"

// Use the namespaces defined for the API and internal classes.
using namespace limestone::api;
using namespace limestone::internal;

namespace limestone {
namespace testing {

// Test fixture for blob_file_gc_snapshot.
class blob_file_gc_snapshot_test : public ::testing::Test {
protected:
    std::string temp_dir;
    int file_counter = 0;

    // Set up a temporary directory for creating log_entry instances.
    void SetUp() override {
        if (system("rm -rf /tmp/limestone_blob_file_gc_snapshot_test") != 0) {
            std::cerr << "Failed to remove /tmp/limestone_blob_file_gc_snapshot_test" << std::endl;
        }
        if (system("mkdir -p /tmp/limestone_blob_file_gc_snapshot_test") != 0) {
            std::cerr << "Failed to create /tmp/limestone_blob_file_gc_snapshot_test" << std::endl;
        }
        temp_dir = "/tmp/limestone_blob_file_gc_snapshot_test";
        file_counter = 0;
    }

    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }

    // Generate a unique temporary file name in temp_dir.
    std::string get_temp_file_name() {
        file_counter++;
        return temp_dir + "/temp_file_" + std::to_string(file_counter);
    }

    /**
     * @brief Creates a blob log entry using write_with_blob.
     *
     * This helper creates a log_entry with type normal_with_blob.
     * The entry is written to a temporary file and then read back.
     *
     * @param storage Storage ID.
     * @param key The key.
     * @param value The value (payload appended after the write_version header).
     * @param wv The write_version.
     * @param blob_ids (Optional) vector of blob_id; default is empty.
     * @return log_entry The created log entry.
     */
    log_entry create_blob_log_entry(storage_id_type storage,
                                    const std::string &key,
                                    const std::string &value,
                                    const write_version_type &wv,
                                    const std::vector<blob_id_type>& blob_ids = {}) {
        std::string temp_file = get_temp_file_name();
        FILE* out = std::fopen(temp_file.c_str(), "wb");
        if (!out) {
            throw std::runtime_error("Failed to open temporary file for writing.");
        }
        // Use the new signature of write_with_blob.
        log_entry::write_with_blob(out, storage, key, value, wv, blob_ids);
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
            throw std::runtime_error("Failed to read blob log entry.");
        }
        return entry;
    }

    // Helper function to check log_entry fields.
    // Checking order: storage_id, key, value, write_version.
    void check_log_entry(const log_entry& entry,
                         storage_id_type expected_storage,
                         const std::string& expected_key,
                         const std::string& expected_value,
                         const write_version_type& expected_wv) {
        EXPECT_EQ(entry.storage(), expected_storage);

        std::string key_buf;
        entry.key(key_buf);
        EXPECT_EQ(key_buf, expected_key);

        std::string value_buf;
        entry.value(value_buf);
        EXPECT_EQ(value_buf, expected_value);

        write_version_type actual_wv;
        entry.write_version(actual_wv);
        EXPECT_EQ(actual_wv, expected_wv);
    }
};

/**
 * @brief Test that a valid blob entry is sanitized and added.
 *
 * The sanitize_and_add_entry() method should truncate the value portion (leaving only
 * the write_version header) and add the entry if its write_version is below the threshold.
 */
TEST_F(blob_file_gc_snapshot_test, sanitize_and_add_entry_valid) {
    storage_id_type storage = 100;
    std::string key = "testKey";
    std::string value = "testValue"; // This payload should be truncated.
    write_version_type wv(50, 1);      // Entry write_version.
    
    // Create a blob log entry.
    log_entry entry = create_blob_log_entry(storage, key, value, wv);
    
    // Verify that the entry is of type normal_with_blob and value_etc contains header + payload.
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_with_blob);
    constexpr std::size_t headerSize = sizeof(epoch_id_type) + sizeof(std::uint64_t);
    EXPECT_EQ(entry.value_etc().size(), headerSize + value.size());
    
    // Create a blob_file_gc_snapshot with a threshold higher than the entry's write_version.
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry);
    snapshot.finalize_local_entries();
    const log_entry_container& snap = snapshot.finalize_snapshot();
    
    // Expect one entry in the snapshot.
    EXPECT_EQ(snap.size(), 1u);
    
    // The added entry should have its value truncated (i.e. extracted value is empty),
    // while storage, key, and write_version remain unchanged.
    log_entry sanitized_entry = *snap.begin();
    std::string extracted_value;
    sanitized_entry.value(extracted_value);
    EXPECT_TRUE(extracted_value.empty());
    check_log_entry(sanitized_entry, storage, key, "", wv);
}

/**
 * @brief Test that a non-blob entry is ignored.
 *
 * Entries that are not of type normal_with_blob should not be added to the snapshot.
 */
TEST_F(blob_file_gc_snapshot_test, sanitize_and_add_entry_invalid_type) {
    // Create a normal entry (using log_entry::write(), not a blob entry).
    storage_id_type storage = 200;
    std::string key = "normalKey";
    std::string value = "normalValue";
    write_version_type wv(50, 1);
    
    std::string temp_file = get_temp_file_name();
    FILE* out = std::fopen(temp_file.c_str(), "wb");
    ASSERT_NE(out, nullptr);
    log_entry::write(out, storage, key, value, wv);
    std::fclose(out);
    std::ifstream in(temp_file, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    log_entry entry;
    bool rc = entry.read(in);
    in.close();
    boost::filesystem::remove(temp_file);
    ASSERT_TRUE(rc);
    
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_entry);
    
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry);
    snapshot.finalize_local_entries();
    const log_entry_container& snap = snapshot.finalize_snapshot();
    
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
    snapshot.sanitize_and_add_entry(entry);
    snapshot.finalize_local_entries();
    const log_entry_container& snap = snapshot.finalize_snapshot();
    EXPECT_EQ(snap.size(), 1u);
    
    // Reset the snapshot.
    snapshot.reset();
    
    // After reset, finalize_snapshot should return an empty snapshot.
    const log_entry_container& snap2 = snapshot.finalize_snapshot();
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
    snapshot.sanitize_and_add_entry(entry1);
    snapshot.sanitize_and_add_entry(entry2);
    snapshot.finalize_local_entries();
    const log_entry_container& snap = snapshot.finalize_snapshot();
    
    // After merging and duplicate removal, only one entry should remain.
    EXPECT_EQ(snap.size(), 1u);
    
    // The remaining entry should have the maximum write_version (wv_high).
    log_entry merged_entry = *snap.begin();
    write_version_type merged_wv;
    merged_entry.write_version(merged_wv);
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
    const log_entry_container& snap1 = snapshot.finalize_snapshot();
    EXPECT_EQ(snap1.size(), 0u);

    // Now, create a valid blob entry.
    storage_id_type storage = 500;
    std::string key = "boundaryKey";
    std::string value = "boundaryValue";
    write_version_type wv(50, 1);
    log_entry entry = create_blob_log_entry(storage, key, value, wv);

    // Calling sanitize_and_add_entry() when tls_container_ is nullptr should not crash
    // and should create a new container to add the entry.
    EXPECT_NO_THROW(snapshot.sanitize_and_add_entry(entry));
    snapshot.finalize_local_entries();
    const log_entry_container& snap2 = snapshot.finalize_snapshot();
    EXPECT_EQ(snap2.size(), 1u);
}

TEST_F(blob_file_gc_snapshot_test, threshold_boundary_test) {
    // Create a snapshot with threshold write_version (100, 1)
    blob_file_gc_snapshot snapshot(write_version_type(100, 1));
    EXPECT_EQ(snapshot.boundary_version(), write_version_type(100, 1));

    // Case 1: Entry with write_version exactly equal to threshold.
    // Expected: The entry should NOT be added (because it's not less than the threshold).
    log_entry entry_equal = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(100, 1));
    snapshot.sanitize_and_add_entry(entry_equal);
    snapshot.finalize_local_entries();
    const log_entry_container& snap_equal = snapshot.finalize_snapshot();
    EXPECT_EQ(snap_equal.size(), 0u);

    // Reset snapshot state.
    snapshot.reset();

    // Case 2: Entry with write_version just below the threshold.
    // For example, (100, 0) is less than (100, 1); expected: the entry should be added.
    log_entry entry_lower = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(100, 0));
    snapshot.sanitize_and_add_entry(entry_lower);
    snapshot.finalize_local_entries();
    const log_entry_container& snap_lower = snapshot.finalize_snapshot();
    EXPECT_EQ(snap_lower.size(), 1u);

    // Reset snapshot state.
    snapshot.reset();

    // Case 3: Entry with write_version above the threshold.
    // For example, (101, 0) is greater than (100, 1); expected: the entry should NOT be added.
    log_entry entry_higher = create_blob_log_entry(600, "boundaryKey", "boundaryValue", write_version_type(101, 0));
    snapshot.sanitize_and_add_entry(entry_higher);
    snapshot.finalize_local_entries();
    const log_entry_container& snap_higher = snapshot.finalize_snapshot();
    EXPECT_EQ(snap_higher.size(), 0u);
}



} // namespace testing
} // namespace limestone
