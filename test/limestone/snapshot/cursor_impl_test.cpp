/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include <boost/filesystem.hpp>
#include "cursor_impl.h"


#include "test_root.h"
#include "limestone/api/log_channel.h"

namespace limestone::testing {

using limestone::api::log_channel;

class cursor_impl_testable : public  limestone::internal::cursor_impl {
public:
    using cursor_impl::cursor_impl;
    using cursor_impl::next;
    using cursor_impl::validate_and_read_stream;
    using cursor_impl::open;
    using cursor_impl::close;
    using cursor_impl::storage;
    using cursor_impl::key;    
    using cursor_impl::value;  
    using cursor_impl::type;
    using cursor_impl::blob_ids;

    ~cursor_impl_testable() {
        // Ensure that the close() method is called to release resources.
        // Normally, resources would be released explicitly, but for this test, we ensure that close()
        // is always called by invoking it in the destructor. This is important to avoid resource leaks
        // in cases where the test does not explicitly call close().
        close();
    }
};

class entry_maker {
public:
    entry_maker& init() {
        entries_.clear();
        return *this;
    }

    entry_maker& add_entry(limestone::api::storage_id_type storage_id, std::string key, std::string value, limestone::api::write_version_type write_version) {
        entries_.emplace_back(storage_id, key, value, write_version);
        return *this;
    }

    std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> get_default_entries() {
        return {
            {1, "key1", "value1", {1, 0}},
            {1, "key2", "value2", {1, 1}}
        };
    }

    const std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>>& get_entries() const {
        return entries_;
    }

private:
    std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries_;
};

class cursor_impl_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/cursor_impl_test";
    std::unique_ptr<limestone::api::datastore_test> datastore_;
    log_channel* lc0_{};
    entry_maker entry_maker_;

    void SetUp() override {
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
        gen_datastore();
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel(location);

        datastore_->ready();
    }

    void TearDown() override {
        datastore_ = nullptr;
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
    }

    void create_log_file(
        const std::string& new_filename,
        const std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>>& entries) {

        lc0_->begin_session();

        for (const auto& entry : entries) {
            lc0_->add_entry(std::get<0>(entry), std::get<1>(entry), std::get<2>(entry), std::get<3>(entry));
        }

        lc0_->end_session();

        boost::filesystem::path pwal_file = boost::filesystem::path(location) / "pwal_0000";
        boost::filesystem::path new_file = boost::filesystem::path(location) / new_filename;

        if (boost::filesystem::exists(pwal_file)) {
            boost::filesystem::rename(pwal_file, new_file);
        } else {
            std::cerr << "Error: pwal_0000 file not found for renaming." << std::endl;
        }
    }
};



// Only Snapshot exists
TEST_F(cursor_impl_test, snapshot_only) {
    create_log_file("snapshot", entry_maker_.get_default_entries());
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";

    cursor_impl_testable cursor(snapshot_file);
    EXPECT_TRUE(cursor.next()) << "Should be able to read the snapshot";
}

// Both Snapshot and Compacted files exist
TEST_F(cursor_impl_test, snapshot_and_compacted) {
    create_log_file("snapshot", entry_maker_.get_default_entries());
    create_log_file("compacted", entry_maker_.get_default_entries());

    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted";

    cursor_impl_testable cursor(snapshot_file, compacted_file);
    EXPECT_TRUE(cursor.next()) << "Should be able to read both snapshot and compacted files";
}

// Error cases
TEST_F(cursor_impl_test, error_case) {
    // No files exist, should throw limestone_exception
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "not_existing_snapshot";
    EXPECT_THROW({
        cursor_impl_testable cursor{boost::filesystem::path(snapshot_file)}; 
    }, limestone::limestone_exception) << "No files should result in a limestone_exception being thrown";

    // Expect the next() method to throw a limestone_exception
    {    
        cursor_impl_testable cursor{boost::filesystem::path(location)}; 
        EXPECT_THROW({
            cursor.next();
        }, limestone::limestone_exception) << "No files should result in a limestone_exception being thrown";
    }
    // invalid sort order
    {
        entry_maker_.init()
            .add_entry(1, "key2", "value2", {1, 1})
            .add_entry(1, "key1", "value1", {1, 0})
            .add_entry(1, "key3", "value3", {1, 2});
        boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";
        create_log_file("snapshot", entry_maker_.get_entries());
        cursor_impl_testable cursor{boost::filesystem::path(snapshot_file)}; 
        EXPECT_THROW({
            while (cursor.next());
        }, limestone::limestone_exception) << "No files should result in a limestone_exception being thrown";
    }
}

// Verify the entry methods after reading from a snapshot file
TEST_F(cursor_impl_test, verify_entry_methods) {
    // Create a snapshot file with default entries
    create_log_file("snapshot", entry_maker_.get_default_entries());
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";

    // Use cursor_impl_testable to read the file
    cursor_impl_testable cursor(snapshot_file);

    // Verify the first entry
    ASSERT_TRUE(cursor.next()) << "First entry should be read";

    // Verify storage() method
    EXPECT_EQ(cursor.storage(), 1) << "Storage ID should be 1";

    // Verify key() method
    std::string key;
    cursor.key(key);
    EXPECT_EQ(key, "key1") << "First key should be 'key1'";

    // Verify value() method
    std::string value;
    cursor.value(value);
    EXPECT_EQ(value, "value1") << "First value should be 'value1'";

    // Verify type() method
    EXPECT_EQ(cursor.type(), limestone::api::log_entry::entry_type::normal_entry)
        << "First entry type should be normal_entry";

    // Verify the second entry
    ASSERT_TRUE(cursor.next()) << "Second entry should be read";

    // Verify storage() method for the second entry
    EXPECT_EQ(cursor.storage(), 1) << "Storage ID should be 1";

    // Verify key() method for the second entry
    cursor.key(key);
    EXPECT_EQ(key, "key2") << "Second key should be 'key2'";

    // Verify value() method for the second entry
    cursor.value(value);
    EXPECT_EQ(value, "value2") << "Second value should be 'value2'";

    // Verify type() method for the second entry
    EXPECT_EQ(cursor.type(), limestone::api::log_entry::entry_type::normal_entry)
        << "Second entry type should be normal_entry";

    // Verify that next() returns false when no more entries are available
    EXPECT_FALSE(cursor.next()) << "No more entries should be available, next() should return false";
}

// Test create_cursor (snapshot only) with clear_storage filtering
TEST_F(cursor_impl_test, create_cursor_snapshot_only_clear_storage_filtering) {
    // Create a snapshot file with default entries:
    // Entry 1: storage 1, key "key1", value "value1", write_version {1, 0}
    // Entry 2: storage 1, key "key2", value "value2", write_version {1, 1}
    create_log_file("snapshot", entry_maker_.get_default_entries());
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";

    // Set clear_storage such that entries with write_version less than {1,1} are filtered out.
    // This should filter out entry "key1" and only allow entry "key2".
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage;
    clear_storage[1] = limestone::api::write_version_type(1, 1);

    // Use the static create_cursor method to create a cursor instance with snapshot file only.
    auto cursor_ptr = limestone::internal::cursor_impl::create_cursor(snapshot_file, clear_storage);

    // Expect that the cursor returns a valid entry.
    ASSERT_TRUE(cursor_ptr->next()) << "Should read a valid entry after filtering.";
    
    // Verify that the key of the returned entry is "key2".
    std::string key;
    cursor_ptr->key(key);
    EXPECT_EQ(key, "key2") << "Expected the remaining entry to have key 'key2'.";

    // There should be no further entries.
    EXPECT_FALSE(cursor_ptr->next()) << "No more entries should be available.";
}

// Test create_cursor (snapshot and compacted) with clear_storage filtering
TEST_F(cursor_impl_test, create_cursor_snapshot_and_compacted_clear_storage_filtering) {
    // Create snapshot and compacted files with default entries.
    create_log_file("snapshot", entry_maker_.get_default_entries());
    create_log_file("compacted", entry_maker_.get_default_entries());
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted";

    // Set clear_storage: for storage 1, set threshold to {1,1} so that entry with write_version {1,0} is filtered out.
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage;
    clear_storage[1] = limestone::api::write_version_type(1, 1);

    // Use the static create_cursor method that accepts both snapshot and compacted files.
    auto cursor_ptr = limestone::internal::cursor_impl::create_cursor(snapshot_file, compacted_file, clear_storage);

    // Expect that the cursor returns a valid entry (which should be the one with key "key2").
    ASSERT_TRUE(cursor_ptr->next()) << "Should read a valid entry from combined files after filtering.";
    
    // Verify the key.
    std::string key;
    cursor_ptr->key(key);
    EXPECT_EQ(key, "key2") << "Expected the remaining entry to have key 'key2'.";

    // Verify that there are no further entries.
    EXPECT_FALSE(cursor_ptr->next()) << "No more entries should be available.";
}

// Validate stream closure on EOF condition
TEST_F(cursor_impl_test, validate_stream_eof) {
    // Create an empty file to simulate immediate EOF.
    boost::filesystem::path empty_file = boost::filesystem::path(location) / "empty_snapshot";
    {
        std::ofstream ofs(empty_file.string());
        ofs.close();
    }

    // Open the empty file into a stream.
    std::optional<boost::filesystem::ifstream> stream;
    stream.emplace(empty_file, std::ios::binary);
    ASSERT_TRUE(stream->is_open());

    // Create dummy log_entry and previous key to pass to validate_and_read_stream.
    std::optional<limestone::api::log_entry> log_entry;
    std::string previous_key;

    // Use cursor_impl_testable to call validate_and_read_stream.
    cursor_impl_testable test_cursor(empty_file);
    test_cursor.validate_and_read_stream(stream, "empty_stream", log_entry, previous_key);

    // Expect that the stream is closed and reset to nullopt due to EOF.
    EXPECT_FALSE(stream.has_value()) << "stream should be closed and reset when EOF is reached";
}

// Validate stream closure when stream is not good
TEST_F(cursor_impl_test, validate_stream_not_good) {
    // Create a file with dummy content.
    boost::filesystem::path bad_file = boost::filesystem::path(location) / "bad_file";
    {
        std::ofstream ofs(bad_file.string());
        ofs << "dummy data";
        ofs.close();
    }

    // Open the file into a stream.
    std::optional<boost::filesystem::ifstream> stream;
    stream.emplace(bad_file, std::ios::binary);
    ASSERT_TRUE(stream->is_open());

    // Force the stream into a bad state.
    stream->setstate(std::ios::failbit);

    // Prepare dummy log_entry and previous key.
    std::optional<limestone::api::log_entry> log_entry;
    std::string previous_key;

    // Use cursor_impl_testable to call validate_and_read_stream.
    cursor_impl_testable test_cursor(bad_file);
    test_cursor.validate_and_read_stream(stream, "bad_stream", log_entry, previous_key);

    // Expect that the stream is closed and reset to nullopt due to not being in a good state.
    EXPECT_FALSE(stream.has_value()) << "stream should be closed and reset when not in a good state";
}

// Verify while loop processes multiple entries with sorted keys
TEST_F(cursor_impl_test, while_loop_processes_multiple_entries_sorted) {
    // Begin a session to write multiple entries into one file.
    lc0_->begin_session();
    // Write a non-relevant entry: remove_entry with key "a"
    lc0_->remove_entry(1, "a", {1, 0});
    // Write another non-relevant entry: remove_entry with key "b"
    lc0_->remove_entry(1, "b", {1, 0});
    // Write a relevant entry: normal_entry with key "d"
    lc0_->add_entry(1, "d", "normal_value", {1, 1});
    // Write a relevant entry: blob entry with key "e" (this produces a normal_with_blob entry)
    lc0_->add_entry(1, "e", "blob_value", {1, 2}, {3001, 3002});
    // Write one more non-relevant entry: remove_entry with key "f"
    lc0_->remove_entry(1, "f", {1, 0});
    lc0_->end_session();

    // Rename the generated PWAL file to "snapshot_multi_sorted"
    boost::filesystem::path pwal_file = boost::filesystem::path(location) / "pwal_0000";
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_multi_sorted";
    if (boost::filesystem::exists(pwal_file)) {
        boost::filesystem::rename(pwal_file, snapshot_file);
    } else {
        FAIL() << "pwal_0000 file not found for renaming";
    }

    // Create a cursor using the snapshot file.
    cursor_impl_testable test_cursor(snapshot_file);

    // The first call to next() should skip the non-relevant entries ("a" and "b")
    // and return the first relevant entry: normal_entry with key "d".
    ASSERT_TRUE(test_cursor.next()) << "Expected to read first relevant normal entry after skipping non-relevant entries";
    std::string key;
    test_cursor.key(key);
    EXPECT_EQ(key, "d") << "Expected first relevant entry key to be 'd'";

    // The second call to next() should return the next relevant entry,
    // which is the blob entry (normal_with_blob) with key "e".
    ASSERT_TRUE(test_cursor.next()) << "Expected to read second relevant blob entry";
    test_cursor.key(key);
    EXPECT_EQ(key, "e") << "Expected second relevant entry key to be 'e'";
    std::vector<limestone::api::blob_id_type> blob_ids = test_cursor.blob_ids();
    std::vector<limestone::api::blob_id_type> expected_blob_ids = {3001, 3002};
    EXPECT_EQ(blob_ids, expected_blob_ids) << "Expected blob IDs to match the provided values";

    // The third call to next() should return false since no further relevant entries exist.
    EXPECT_FALSE(test_cursor.next()) << "Expected no further relevant entries";
}

// Verify that an invalid (non-relevant) entry is reset and skipped.
TEST_F(cursor_impl_test, while_loop_resets_invalid_entry) {
    // Create a snapshot file with a single normal_entry.
    lc0_->begin_session();
    lc0_->add_entry(1, "irrelevant_key", "irrelevant_value", {1, 0});
    lc0_->end_session();

    // Rename the generated PWAL file to "snapshot_invalid"
    boost::filesystem::path pwal_file = boost::filesystem::path(location) / "pwal_0000";
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_invalid";
    if (boost::filesystem::exists(pwal_file)) {
        boost::filesystem::rename(pwal_file, snapshot_file);
    } else {
        FAIL() << "pwal_0000 file not found for renaming";
    }

    // Create a cursor using the snapshot file.
    cursor_impl_testable test_cursor(snapshot_file);

    // Set clear_storage to a threshold that makes the entry non-relevant.
    // For example, if the entry's write version is {1, 0}, setting the threshold to {1, 1} 
    // will cause the entry to be considered outdated (non-relevant).
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage;
    clear_storage[1] = limestone::api::write_version_type(1, 1);
    test_cursor.set_clear_storage(clear_storage);

    // Call next(). The while loop in validate_and_read_stream will read the single entry,
    // find it non-relevant, reset log_entry, and eventually return false since no further entries exist.
    EXPECT_FALSE(test_cursor.next()) << "Expected next() to return false when only non-relevant entries are present";
}

// The entries are inserted in order: first by storage ID, then by key, then by write_version.
TEST_F(cursor_impl_test, skip_non_target_entries_sorted) {
    // Begin a session to write multiple entries.
    lc0_->begin_session();
    // For storage ID 1 (target entries):
    // Write a normal_entry with key "a"
    lc0_->add_entry(1, "a", "value_a", {1, 0});
    // Write a normal_with_blob entry with key "b"
    lc0_->add_entry(1, "b", "value_b", {1, 1}, {2001, 2002});
    // Write a remove_entry with key "c"
    lc0_->remove_entry(1, "c", {1, 2});
    // Insert non-target entry for storage ID 1: clear_storage entry via truncate_storage.
    lc0_->truncate_storage(1, {1, 3});
    // For storage ID 2: non-target entry via add_storage.
    lc0_->add_storage(2, {1, 4});
    // For storage ID 3: non-target entry via remove_storage.
    lc0_->remove_storage(3, {1, 5});
    lc0_->end_session();

    // Rename the generated PWAL file to "snapshot_sorted"
    boost::filesystem::path pwal_file = boost::filesystem::path(location) / "pwal_0000";
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_sorted";
    if (boost::filesystem::exists(pwal_file)) {
        boost::filesystem::rename(pwal_file, snapshot_file);
    } else {
        FAIL() << "pwal_0000 file not found for renaming";
    }

    // Create a cursor using the snapshot file.
    cursor_impl_testable test_cursor(snapshot_file);

    // Expected target entries (from storage ID 1) in sorted order:
    // "a" (normal_entry), "b" (normal_with_blob), "c" (remove_entry).
    std::vector<std::pair<std::string, std::string>> expected_entries = {
        {"a", "value_a"},
        {"b", "value_b"},
    };

    std::vector<std::pair<std::string, std::string>> actual_entries;
    while (test_cursor.next()) {
        std::string key, value;
        test_cursor.key(key);
        test_cursor.value(value);
        actual_entries.emplace_back(key, value);
    }

    EXPECT_EQ(actual_entries, expected_entries)
        << "Only target entries (normal_entry, normal_with_blob, remove_entry) should be processed";
}

// Test case: Both snapshot and compacted exist with snapshot key less than compacted key.
// In this case, the cursor should use the snapshot entry.
TEST_F(cursor_impl_test, both_exist_snapshot_lt_compacted) {
    // Create snapshot file with a single entry with key "aaa"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "aaa", "val_snapshot", {1, 0}}
        };
        create_log_file("snapshot_aaa", entries);
    }
    // Create compacted file with a single entry with key "bbb"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "bbb", "val_compacted", {1, 1}}
        };
        create_log_file("compacted_bbb", entries);
    }
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_aaa";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted_bbb";

    // Create a cursor with both snapshot and compacted files.
    cursor_impl_testable test_cursor(snapshot_file, compacted_file);
    // Since "aaa" < "bbb", the first call to next() should return the snapshot entry.
    ASSERT_TRUE(test_cursor.next());
    std::string key;
    test_cursor.key(key);
    EXPECT_EQ(key, "aaa") << "Expected snapshot entry (key 'aaa') when snapshot key < compacted key";
}

// Test case: Both snapshot and compacted exist with snapshot key greater than compacted key.
// In this case, the cursor should use the compacted entry.
TEST_F(cursor_impl_test, both_exist_snapshot_gt_compacted) {
    // Create snapshot file with a single entry with key "ccc"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "ccc", "val_snapshot", {1, 0}}
        };
        create_log_file("snapshot_ccc", entries);
    }
    // Create compacted file with a single entry with key "bbb"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "bbb", "val_compacted", {1, 1}}
        };
        create_log_file("compacted_bbb", entries);
    }
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_ccc";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted_bbb";

    // Create a cursor with both files.
    cursor_impl_testable test_cursor(snapshot_file, compacted_file);
    // Since "ccc" > "bbb", the first call to next() should return the compacted entry.
    ASSERT_TRUE(test_cursor.next());
    std::string key;
    test_cursor.key(key);
    EXPECT_EQ(key, "bbb") << "Expected compacted entry (key 'bbb') when snapshot key > compacted key";
}

// Test case: Both snapshot and compacted exist with equal keys.
// In this case, the cursor should use the snapshot entry and reset both.
TEST_F(cursor_impl_test, both_exist_equal_keys) {
    // Create snapshot file with a single entry with key "ddd"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "ddd", "val_snapshot", {1, 0}}
        };
        create_log_file("snapshot_ddd", entries);
    }
    // Create compacted file with a single entry with key "ddd"
    {
        std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
            {1, "ddd", "val_compacted", {1, 1}}
        };
        create_log_file("compacted_ddd", entries);
    }
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_ddd";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted_ddd";

    // Create a cursor with both files.
    cursor_impl_testable test_cursor(snapshot_file, compacted_file);
    // When keys are equal, the code uses the snapshot entry and resets both.
    ASSERT_TRUE(test_cursor.next());
    std::string key;
    test_cursor.key(key);
    EXPECT_EQ(key, "ddd") << "Expected snapshot entry (key 'ddd') when snapshot and compacted keys are equal";
}

// Verify that when the snapshot file yields no log entry but
// the compacted file yields a valid entry, the cursor uses the compacted entry.
TEST_F(cursor_impl_test, use_compacted_when_snapshot_empty) {
    // Create an empty snapshot file to simulate no valid log entry.
    boost::filesystem::path empty_snapshot = boost::filesystem::path(location) / "empty_snapshot";
    {
        std::ofstream ofs(empty_snapshot.string());
        ofs.close();
    }

    // Create a compacted file with one valid log entry.
    std::vector<std::tuple<limestone::api::storage_id_type, std::string, std::string, limestone::api::write_version_type>> entries = {
        {1, "compacted_key", "compacted_value", {1, 0}}
    };
    create_log_file("compacted_file", entries);
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted_file";

    // Create a cursor using both files: an empty snapshot and a valid compacted file.
    cursor_impl_testable test_cursor(empty_snapshot, compacted_file);

    // Since the snapshot stream yields no entry, the compacted stream's entry should be used.
    ASSERT_TRUE(test_cursor.next()) << "Expected next() to return true when compacted file provides a valid entry";
    std::string key;
    test_cursor.key(key);
    EXPECT_EQ(key, "compacted_key") << "Expected the entry from the compacted file when snapshot is empty";
}

// Test case for skipping duplicate key_sid entries in validate_and_read_stream
TEST_F(cursor_impl_test, skip_duplicate_key) {
    entry_maker_.init()
        .add_entry(1, "dup", "first", {1, 0})
        .add_entry(1, "dup", "second", {1, 1})
        .add_entry(1, "unique", "third", {1, 2});
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot_duplicate";
    create_log_file("snapshot_duplicate", entry_maker_.get_entries());

    cursor_impl_testable cursor(snapshot_file);

    // The first call to next() should return the first "dup" entry.
    ASSERT_TRUE(cursor.next()) << "Expected to read the first entry with key 'dup'";
    std::string key;
    cursor.key(key);
    EXPECT_EQ(key, "dup") << "Expected key to be 'dup'";

    // The second call to next() should skip the duplicate "dup" and return the "unique" entry.
    ASSERT_TRUE(cursor.next()) << "Expected to read the next entry after skipping duplicate";
    cursor.key(key);
    EXPECT_EQ(key, "unique") << "Expected key to be 'unique' after skipping duplicate";

    // There should be no further entries.
    EXPECT_FALSE(cursor.next()) << "Expected no further entries";
}

}  // namespace limestone::testing
