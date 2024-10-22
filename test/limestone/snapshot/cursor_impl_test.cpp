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



// Test case 1: Only Snapshot exists
TEST_F(cursor_impl_test, snapshot_only) {
    create_log_file("snapshot", entry_maker_.get_default_entries());
    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";

    cursor_impl_testable cursor(snapshot_file);
    EXPECT_TRUE(cursor.next()) << "Should be able to read the snapshot";
}

// Test case 2: Both Snapshot and Compacted files exist
TEST_F(cursor_impl_test, snapshot_and_compacted) {
    create_log_file("snapshot", entry_maker_.get_default_entries());
    create_log_file("compacted", entry_maker_.get_default_entries());

    boost::filesystem::path snapshot_file = boost::filesystem::path(location) / "snapshot";
    boost::filesystem::path compacted_file = boost::filesystem::path(location) / "compacted";

    cursor_impl_testable cursor(snapshot_file, compacted_file);
    EXPECT_TRUE(cursor.next()) << "Should be able to read both snapshot and compacted files";
}

// Test case 3: Error cases
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

// Test Case 4: Verify the entry methods after reading from a snapshot file
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



}  // namespace limestone::testing
