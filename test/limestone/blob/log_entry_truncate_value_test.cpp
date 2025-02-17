#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <sstream>
#include <vector>
#include <boost/filesystem.hpp>
#include "log_entry.h"

using namespace limestone::api;

namespace limestone::testing {

// Test fixture for testing truncate_value_from_normal_entry()
class log_entry_truncate_value_test : public ::testing::Test {
protected:
    std::string temp_dir;
    int file_counter = 0;

    void SetUp() override {
        // Remove any previous test directory.
        if (system("rm -rf /tmp/limestone_log_entry_truncate_value_test") != 0) {
            std::cerr << "Failed to remove directory /tmp/limestone_log_entry_truncate_value_test" << std::endl;
        }
        // Create the test directory.
        if (system("mkdir -p /tmp/limestone_log_entry_truncate_value_test") != 0) {
            std::cerr << "Failed to create directory /tmp/limestone_log_entry_truncate_value_test" << std::endl;
        }
        temp_dir = "/tmp/limestone_log_entry_truncate_value_test";
        file_counter = 0;
    }

    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }

    std::string get_temp_file_name() {
        file_counter++;
        return temp_dir + "/temp_file_" + std::to_string(file_counter);
    }
};

// Helper: Returns the header size for write_version (epoch_id_type + std::uint64_t).
constexpr std::size_t header_size() {
    return sizeof(epoch_id_type) + sizeof(std::uint64_t);
}

/**
 * @brief Verify that for a normal_entry created with log_entry::write(),
 *        truncate_value_from_normal_entry() removes the appended value data
 *        (resulting in an empty extracted value), while storage_id, key,
 *        and write_version remain unchanged.
 *
 * The checking order is: storage_id, key, value, write_version.
 */
TEST_F(log_entry_truncate_value_test, truncate_value_normal_entry) {
    // Prepare test data.
    storage_id_type storage = 123;
    std::string key = "testKey";
    std::string value = "testValue"; // The value part appended after the header.
    write_version_type wv(100, 10);

    // Write a normal entry using log_entry::write().
    std::string temp_file = get_temp_file_name();
    FILE* out = std::fopen(temp_file.c_str(), "wb");
    ASSERT_NE(out, nullptr);
    log_entry::write(out, storage, key, value, wv);
    std::fclose(out);

    // Read the entry back.
    std::ifstream in(temp_file, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    log_entry entry;
    bool rc = entry.read(in);
    in.close();
    boost::filesystem::remove(temp_file);
    ASSERT_TRUE(rc);

    // Verify that the entry is of type normal_entry and that value_etc contains header + value.
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_entry);
    EXPECT_EQ(entry.value_etc().size(), header_size() + value.size());

    // Capture fields before truncation in order: storage_id, key, value, write_version.
    storage_id_type storage_before = entry.storage();
    std::string key_before;
    entry.key(key_before);
    std::string value_before;
    entry.value(value_before);
    write_version_type wv_before;
    entry.write_version(wv_before);

    EXPECT_EQ(storage_before, storage);
    EXPECT_EQ(key_before, key);
    EXPECT_EQ(value_before, value);
    EXPECT_EQ(wv_before.get_major(), wv.get_major());
    EXPECT_EQ(wv_before.get_minor(), wv.get_minor());

    // Invoke truncation.
    entry.truncate_value_from_normal_entry();

    // After truncation, only the header should remain.
    EXPECT_EQ(entry.value_etc().size(), header_size());

    // Verify that storage_id, key, and write_version remain unchanged.
    storage_id_type storage_after = entry.storage();
    std::string key_after;
    entry.key(key_after);
    std::string value_after;
    entry.value(value_after);
    write_version_type wv_after;
    entry.write_version(wv_after);

    EXPECT_EQ(storage_after, storage_before);
    EXPECT_EQ(key_after, key_before);
    EXPECT_EQ(wv_after.get_major(), wv_before.get_major());
    EXPECT_EQ(wv_after.get_minor(), wv_before.get_minor());
    // The extracted value should now be empty.
    EXPECT_TRUE(value_after.empty());
}

/**
 * @brief Verify that for a normal_with_blob entry created with log_entry::write_with_blob(),
 *        truncate_value_from_normal_entry() removes the appended value data
 *        (resulting in an empty extracted value), while storage_id, key,
 *        and write_version remain unchanged.
 *
 * The checking order is: storage_id, key, value, write_version.
 *
 * This test uses the new write_with_blob signature:
 *   write_with_blob(FILE*, storage_id_type, std::string_view key, std::string_view value,
 *                   write_version_type, const std::vector<blob_id_type>& large_objects)
 */
TEST_F(log_entry_truncate_value_test, truncate_value_with_blob) {
    // Prepare test data.
    storage_id_type storage = 456;
    std::string key = "blobKey";
    std::string value = "blobValue"; // The value part appended after the header.
    write_version_type wv(200, 20);
    // Prepare some dummy blob_ids.
    std::vector<blob_id_type> large_objects = { 42, 43, 44 };

    // Write the blob entry using the new write_with_blob.
    std::string temp_file = get_temp_file_name();
    FILE* out = std::fopen(temp_file.c_str(), "wb");
    ASSERT_NE(out, nullptr);
    log_entry::write_with_blob(out, storage, key, value, wv, large_objects);
    std::fclose(out);

    // Read the entry back.
    std::ifstream in(temp_file, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    log_entry entry;
    bool rc = entry.read(in);
    in.close();
    boost::filesystem::remove(temp_file);
    ASSERT_TRUE(rc);

    // Verify that the entry is of type normal_with_blob and that value_etc contains header + value.
    EXPECT_EQ(entry.type(), log_entry::entry_type::normal_with_blob);
    EXPECT_EQ(entry.value_etc().size(), header_size() + value.size());

    // Capture fields before truncation in order: storage_id, key, value, write_version.
    storage_id_type storage_before = entry.storage();
    std::string key_before;
    entry.key(key_before);
    std::string value_before;
    entry.value(value_before);
    write_version_type wv_before;
    entry.write_version(wv_before);

    EXPECT_EQ(storage_before, storage);
    EXPECT_EQ(key_before, key);
    EXPECT_EQ(value_before, value);
    EXPECT_EQ(wv_before.get_major(), wv.get_major());
    EXPECT_EQ(wv_before.get_minor(), wv.get_minor());

    // Invoke truncation.
    entry.truncate_value_from_normal_entry();

    // After truncation, only the header should remain.
    EXPECT_EQ(entry.value_etc().size(), header_size());

    // Verify that storage_id, key, and write_version remain unchanged.
    storage_id_type storage_after = entry.storage();
    std::string key_after;
    entry.key(key_after);
    std::string value_after;
    entry.value(value_after);
    write_version_type wv_after;
    entry.write_version(wv_after);

    EXPECT_EQ(storage_after, storage_before);
    EXPECT_EQ(key_after, key_before);
    EXPECT_EQ(wv_after.get_major(), wv_before.get_major());
    EXPECT_EQ(wv_after.get_minor(), wv_before.get_minor());
    // The extracted value should now be empty.
    EXPECT_TRUE(value_after.empty());
}

/**
 * @brief Verify that for an entry type without a value portion (e.g. marker_begin),
 *        truncate_value_from_normal_entry() does not modify value_etc.
 */
TEST_F(log_entry_truncate_value_test, truncate_value_non_normal_entry) {
    // Create a marker_begin entry.
    std::string temp_file = get_temp_file_name();
    FILE* out = std::fopen(temp_file.c_str(), "wb");
    ASSERT_NE(out, nullptr);
    epoch_id_type epoch = 999;
    log_entry::begin_session(out, epoch);
    std::fclose(out);

    std::ifstream in(temp_file, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    log_entry entry;
    bool rc = entry.read(in);
    in.close();
    boost::filesystem::remove(temp_file);
    ASSERT_TRUE(rc);

    // Capture the original value_etc.
    std::string original_value_etc = entry.value_etc();
    // Invoke truncation.
    entry.truncate_value_from_normal_entry();
    // For entries like marker_begin that do not have a value portion, value_etc should remain unchanged.
    EXPECT_EQ(entry.value_etc(), original_value_etc);
}

} // namespace limestone::testing
