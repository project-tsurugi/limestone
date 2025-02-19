#include <gtest/gtest.h>
#include <limestone/api/datastore.h>
#include <limestone/api/blob_file.h>
#include <limestone/api/log_channel.h>
#include <limits>
#include <string>
#include <memory>
#include <boost/filesystem.hpp>
#include "test_root.h"

namespace limestone::testing {

using limestone::api::log_channel;
using limestone::api::blob_id_type;
using limestone::api::write_version_type;

constexpr const char* data_location = "/tmp/datastore_blob_test/data_location";
constexpr const char* metadata_location = "/tmp/datastore_blob_test/metadata_location";

class datastore_blob_test : public ::testing::Test {
protected:
    std::unique_ptr<api::datastore_test> datastore_;
    boost::filesystem::path location_;
    log_channel* lc0_{};
    log_channel* lc1_{};
    log_channel* lc2_{};

    void SetUp() override {
        if (system("chmod -R a+rwx /tmp/datastore_blob_test") != 0) {
            std::cerr << "cannot change permission" << std::endl;
        }

        if (system("rm -rf /tmp/datastore_blob_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/datastore_blob_test/data_location /tmp/datastore_blob_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        gen_datastore();
    }

    void TearDown() override {
        if (datastore_) {
            datastore_->shutdown();
        }
        boost::filesystem::remove_all(location_);
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &datastore_->create_channel(data_location);
        lc1_ = &datastore_->create_channel(data_location);
        lc2_ = &datastore_->create_channel(data_location);

        datastore_->ready();
    }

    boost::filesystem::path create_dummy_file(api::blob_id_type blob_id) {
        auto file = datastore_->get_blob_file(blob_id);
        auto path = file.path();
        boost::filesystem::create_directories(path.parent_path());
        boost::filesystem::ofstream dummy_file(path);
        dummy_file << "test data";
        dummy_file.close();
        return path;  
    }
};

TEST_F(datastore_blob_test, acquire_blob_pool_basic) {
    auto pool = datastore_->acquire_blob_pool();
    ASSERT_NE(pool, nullptr);
}

TEST_F(datastore_blob_test, acquire_blob_pool_overflow_boundary) {
    // Set next_blob_id to max - 1.
    auto max_id = std::numeric_limits<blob_id_type>::max();
    datastore_->set_next_blob_id(max_id - 1);

    // Acquire the blob pool.
    auto pool = datastore_->acquire_blob_pool();
    ASSERT_NE(pool, nullptr);

    std::string test_data1 = "test data 1";
    std::string test_data2 = "test data 2";

    // First call: the id_generator should return max - 1 and update next_blob_id to max.
    auto id1 = pool->register_data(test_data1);
    EXPECT_EQ(id1, max_id - 1) << "Expected first registered blob ID to be max - 1";

    // Second call: since next_blob_id is now max, the lambda should return max.
    auto id2 = pool->register_data(test_data2);
    EXPECT_EQ(id2, max_id) << "Expected second registered blob ID to be max, indicating overflow";
}



TEST_F(datastore_blob_test, get_blob_file_basic) {
    int next_blob_id = 12345;
    int existing_blob_id = 12344;
    datastore_->set_next_blob_id(next_blob_id);

    // create_dummy_file now returns the generated file's path.
    boost::filesystem::path expected_existing_path = create_dummy_file(existing_blob_id);
    boost::filesystem::path expected_next_path = create_dummy_file(next_blob_id);

    // Case 1: Normal case - file exists and is accessible.
    auto file = datastore_->get_blob_file(existing_blob_id);
    EXPECT_EQ(file.path().string(), expected_existing_path.string())
        << "Returned path should match the dummy file path for existing_blob_id";
    EXPECT_TRUE(static_cast<bool>(file))
        << "File should be available when it exists";

    // Case 2: File is removed after being confirmed to exist.
    boost::filesystem::remove(file.path());
    auto file_removed = datastore_->get_blob_file(existing_blob_id);
    EXPECT_FALSE(static_cast<bool>(file_removed))
        << "File should be marked as unavailable if it has been removed";
    EXPECT_EQ(file_removed.path().string(), expected_existing_path.string())
        << "Returned path should still match the dummy file path even if the file is unavailable";

    // Case 3: Boundary condition - ID equal to next_blob_id.
    auto file_next_blob_id = datastore_->get_blob_file(next_blob_id);
    EXPECT_EQ(file_next_blob_id.path().string(), expected_next_path.string())
        << "Returned path should match the dummy file path for next_blob_id";
    EXPECT_FALSE(static_cast<bool>(file_next_blob_id))
        << "File should be available for next_blob_id if it exists";
}

// Environment-dependent part (disabled in CI environment)
// This test simulates a permission error so that boost::filesystem::exists() fails,
// causing the catch block to mark the file as unavailable.
TEST_F(datastore_blob_test, DISABLED_get_blob_file_filesystem_error) {
    FLAGS_v = 70;
    int existing_blob_id = 12345;
    datastore_->set_next_blob_id(existing_blob_id + 1);
    boost::filesystem::path expected_path = create_dummy_file(existing_blob_id);

    // Normally, the file should exist.
    auto file = datastore_->get_blob_file(existing_blob_id);
    EXPECT_TRUE(boost::filesystem::exists(file.path()))
        << "File should exist before permission change";
    EXPECT_TRUE(static_cast<bool>(file))
        << "File should be available before permission change";
    EXPECT_EQ(file.path().string(), expected_path.string())
        << "Returned path should match the dummy file path";

    // Get the parent directory of the file and save its original permissions.
    boost::filesystem::path parent_dir = file.path().parent_path();
    auto original_perms = boost::filesystem::status(parent_dir).permissions();

    // Set the directory permissions to no permissions to simulate a filesystem error.
    boost::filesystem::permissions(parent_dir, boost::filesystem::perms::no_perms);

    // Now, get_blob_file should mark the file as unavailable.
    auto file_error = datastore_->get_blob_file(existing_blob_id);
    EXPECT_FALSE(static_cast<bool>(file_error))
        << "Expected file to be marked unavailable due to permission error";
    EXPECT_EQ(file_error.path().string(), expected_path.string())
        << "Returned path should still match the dummy file path even if file is unavailable";

    // Restore the original permissions so that subsequent tests are not affected.
    boost::filesystem::permissions(parent_dir, original_perms);
}

TEST_F(datastore_blob_test, add_persistent_blob_ids) {
    std::vector<blob_id_type> blob_ids = {1, 2, 3};
    datastore_->add_persistent_blob_ids(blob_ids);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids.size(), 3);
    EXPECT_TRUE(persistent_blob_ids.find(1) != persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(2) != persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(3) != persistent_blob_ids.end());
}

TEST_F(datastore_blob_test, add_empty_persistent_blob_ids) {
    std::vector<blob_id_type> empty_blob_ids;
    datastore_->add_persistent_blob_ids(empty_blob_ids);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_TRUE(persistent_blob_ids.empty());
}

TEST_F(datastore_blob_test, add_persistent_blob_ids_multiple_calls) {
    // Check initial state
    auto initial_persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_TRUE(initial_persistent_blob_ids.empty());

    // First call
    std::vector<blob_id_type> blob_ids1 = {1, 2, 3};
    datastore_->add_persistent_blob_ids(blob_ids1);

    auto persistent_blob_ids_after_first_call = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids_after_first_call.size(), 3);
    EXPECT_TRUE(persistent_blob_ids_after_first_call.find(1) != persistent_blob_ids_after_first_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_first_call.find(2) != persistent_blob_ids_after_first_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_first_call.find(3) != persistent_blob_ids_after_first_call.end());

    // Call with an empty list
    std::vector<blob_id_type> empty_blob_ids;
    datastore_->add_persistent_blob_ids(empty_blob_ids);

    auto persistent_blob_ids_after_empty_call = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids_after_empty_call.size(), 3); // No change
    EXPECT_TRUE(persistent_blob_ids_after_empty_call.find(1) != persistent_blob_ids_after_empty_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_empty_call.find(2) != persistent_blob_ids_after_empty_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_empty_call.find(3) != persistent_blob_ids_after_empty_call.end());

    // Additional call
    std::vector<blob_id_type> blob_ids2 = {4, 5};
    datastore_->add_persistent_blob_ids(blob_ids2);

    auto persistent_blob_ids_after_second_call = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids_after_second_call.size(), 5);
    EXPECT_TRUE(persistent_blob_ids_after_second_call.find(1) != persistent_blob_ids_after_second_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_second_call.find(2) != persistent_blob_ids_after_second_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_second_call.find(3) != persistent_blob_ids_after_second_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_second_call.find(4) != persistent_blob_ids_after_second_call.end());
    EXPECT_TRUE(persistent_blob_ids_after_second_call.find(5) != persistent_blob_ids_after_second_call.end());
}


TEST_F(datastore_blob_test, check_and_remove_persistent_blob_ids_all_exist) {
    std::vector<blob_id_type> blob_ids_to_add = {1, 2, 3};
    datastore_->add_persistent_blob_ids(blob_ids_to_add);

    std::vector<blob_id_type> blob_ids_to_check_and_remove = {1, 2, 3};
    auto not_found_blob_ids = datastore_->check_and_remove_persistent_blob_ids(blob_ids_to_check_and_remove);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_TRUE(persistent_blob_ids.empty());

    EXPECT_TRUE(not_found_blob_ids.empty());
}

TEST_F(datastore_blob_test, check_and_remove_persistent_blob_ids_some_exist) {
    std::vector<blob_id_type> blob_ids_to_add = {1, 2, 3};
    datastore_->add_persistent_blob_ids(blob_ids_to_add);

    std::vector<blob_id_type> blob_ids_to_check_and_remove = {2, 3, 4};
    auto not_found_blob_ids = datastore_->check_and_remove_persistent_blob_ids(blob_ids_to_check_and_remove);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids.size(), 1);
    EXPECT_TRUE(persistent_blob_ids.find(1) != persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(2) == persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(3) == persistent_blob_ids.end());

    EXPECT_EQ(not_found_blob_ids.size(), 1);
    EXPECT_EQ(not_found_blob_ids[0], 4);
}

TEST_F(datastore_blob_test, check_and_remove_persistent_blob_ids_empty_parameter) {
    std::vector<blob_id_type> blob_ids_to_add = {1, 2, 3};
    datastore_->add_persistent_blob_ids(blob_ids_to_add);

    std::vector<blob_id_type> empty_blob_ids_to_check_and_remove;
    auto not_found_blob_ids = datastore_->check_and_remove_persistent_blob_ids(empty_blob_ids_to_check_and_remove);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_EQ(persistent_blob_ids.size(), 3);
    EXPECT_TRUE(persistent_blob_ids.find(1) != persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(2) != persistent_blob_ids.end());
    EXPECT_TRUE(persistent_blob_ids.find(3) != persistent_blob_ids.end());

    EXPECT_TRUE(not_found_blob_ids.empty());
}

TEST_F(datastore_blob_test, check_and_remove_persistent_blob_ids_empty_persistent_blob_ids) {
    std::vector<blob_id_type> empty_blob_ids_to_check_and_remove = {1, 2, 3};
    auto not_found_blob_ids = datastore_->check_and_remove_persistent_blob_ids(empty_blob_ids_to_check_and_remove);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_TRUE(persistent_blob_ids.empty());

    EXPECT_EQ(not_found_blob_ids.size(), 3);
    EXPECT_TRUE(std::find(not_found_blob_ids.begin(), not_found_blob_ids.end(), 1) != not_found_blob_ids.end());
    EXPECT_TRUE(std::find(not_found_blob_ids.begin(), not_found_blob_ids.end(), 2) != not_found_blob_ids.end());
    EXPECT_TRUE(std::find(not_found_blob_ids.begin(), not_found_blob_ids.end(), 3) != not_found_blob_ids.end());
}

TEST_F(datastore_blob_test, check_and_remove_persistent_blob_ids_both_empty) {
    std::vector<blob_id_type> empty_blob_ids_to_check_and_remove;
    auto not_found_blob_ids = datastore_->check_and_remove_persistent_blob_ids(empty_blob_ids_to_check_and_remove);

    auto persistent_blob_ids = datastore_->get_persistent_blob_ids();
    EXPECT_TRUE(persistent_blob_ids.empty());

    EXPECT_TRUE(not_found_blob_ids.empty());
}

TEST_F(datastore_blob_test, scenario01) {
    auto pool = datastore_->acquire_blob_pool();

    std::string data1 = "test data";
    std::string data2 = "more test data";
    auto blob_id1 = pool->register_data(data1);
    auto blob_id2 = pool->register_data(data2);

    auto blob_fil1 = datastore_->get_blob_file(blob_id1);
    auto blob_fil2 = datastore_->get_blob_file(blob_id2);

    EXPECT_TRUE(boost::filesystem::exists(blob_fil1.path()));
    EXPECT_TRUE(boost::filesystem::exists(blob_fil2.path()));

    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1,1}, {blob_id1});
    lc0_->end_session();

    EXPECT_TRUE(boost::filesystem::exists(blob_fil1.path()));
    EXPECT_TRUE(boost::filesystem::exists(blob_fil2.path()));
    EXPECT_EQ(datastore_->get_persistent_blob_ids().size(), 1);
    EXPECT_TRUE(datastore_->get_persistent_blob_ids().find(blob_id1) != datastore_->get_persistent_blob_ids().end());

    pool->release();

    EXPECT_TRUE(boost::filesystem::exists(blob_fil1.path()));
    EXPECT_FALSE(boost::filesystem::exists(blob_fil2.path()));
    EXPECT_TRUE(datastore_->get_persistent_blob_ids().empty());

    pool->release();

    EXPECT_TRUE(boost::filesystem::exists(blob_fil1.path()));
    EXPECT_FALSE(boost::filesystem::exists(blob_fil2.path()));
    EXPECT_TRUE(datastore_->get_persistent_blob_ids().empty());
}

TEST_F(datastore_blob_test, next_blob_id) {
    // On the first startup, it should be 1
    {
        EXPECT_EQ(datastore_->next_blob_id(), 1);
        auto cursor = datastore_->get_snapshot()->get_cursor();
        EXPECT_FALSE(cursor->next());
    }

    // After restarting without doing anything, it should still be 1
    {
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();

        EXPECT_EQ(datastore_->next_blob_id(), 1);
        auto cursor = datastore_->get_snapshot()->get_cursor();
        EXPECT_FALSE(cursor->next());
    }

    // Add an entry without a BLOB and restart
    {
        lc0_->begin_session();
        lc0_->add_entry(101, "test_key", "test_value", {1, 0});
        lc0_->end_session();
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();

        EXPECT_EQ(datastore_->next_blob_id(), 1);
        auto cursor = datastore_->get_snapshot()->get_cursor();
        EXPECT_TRUE(cursor->next());
        EXPECT_EQ(cursor->storage(), 101);
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        EXPECT_EQ(key, "test_key");
        EXPECT_EQ(value, "test_value");
        EXPECT_FALSE(cursor->next());
    }

    // Add an entry with a BLOB and restart
    {
        lc0_->begin_session();
        lc0_->add_entry(101, "test_key2", "test_value2", {1, 0}, {1001, 1002});
        lc0_->end_session();
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();

        EXPECT_EQ(datastore_->next_blob_id(), 1003);
        auto cursor = datastore_->get_snapshot()->get_cursor();
        EXPECT_TRUE(cursor->next());
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        EXPECT_EQ(key, "test_key");
        EXPECT_EQ(value, "test_value");
        EXPECT_TRUE(cursor->next());
        std::string key2;
        std::string value2;
        cursor->key(key2);
        cursor->value(value2);
        EXPECT_EQ(key2, "test_key2");
        EXPECT_EQ(value2, "test_value2");
        EXPECT_FALSE(cursor->next());
    }

    // Add an entry without a BLOB and restart
    {
        lc0_->begin_session();
        lc0_->add_entry(101, "test_key3", "test_value3", {1, 0});
        lc0_->end_session();
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();

        EXPECT_EQ(datastore_->next_blob_id(), 1003);
        auto cursor = datastore_->get_snapshot()->get_cursor();
        EXPECT_TRUE(cursor->next());
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        EXPECT_EQ(key, "test_key");
        EXPECT_EQ(value, "test_value");
        EXPECT_TRUE(cursor->next());
        std::string key2;
        std::string value2;
        cursor->key(key2);
        cursor->value(value2);
        EXPECT_EQ(key2, "test_key2");
        EXPECT_EQ(value2, "test_value2");
        EXPECT_TRUE(cursor->next());
        std::string key3;
        std::string value3;
        cursor->key(key3);
        cursor->value(value3);
        EXPECT_EQ(key3, "test_key3");
        EXPECT_EQ(value3, "test_value3");
        EXPECT_FALSE(cursor->next());
    }
}

TEST_F(datastore_blob_test, switch_available_boundary_version_basic) {
    write_version_type initial_version(0, 0);
    write_version_type version1(1, 0);
    write_version_type version2(2, 5);
    write_version_type invalid_version(0, 5);  // Boundary version must be monotonically increasing

    // Check initial version
    EXPECT_EQ(datastore_->get_available_boundary_version(), initial_version);

    // Set valid versions
    datastore_->switch_available_boundary_version(version1);
    EXPECT_EQ(datastore_->get_available_boundary_version(), version1);

    datastore_->switch_available_boundary_version(version2);
    EXPECT_EQ(datastore_->get_available_boundary_version(), version2);

    // Attempting to set an invalid version (smaller value) results in an error (version remains unchanged)
    datastore_->switch_available_boundary_version(invalid_version);
    EXPECT_EQ(datastore_->get_available_boundary_version(), version2);
}

TEST_F(datastore_blob_test, available_boundary_version_after_reboot) {
    // Check the initial value after ready() execution
    write_version_type expected_version(0, 0);
    if (datastore_->last_epoch() != 0) {
        expected_version = write_version_type(datastore_->last_epoch(), 0);
    }
    EXPECT_EQ(datastore_->get_available_boundary_version(), expected_version);

    // --- Step 1: Write data ---
    datastore_->switch_epoch(115);
    lc0_->begin_session();
    lc0_->add_entry(101, "test_key", "test_value", {115, 52});
    lc0_->end_session();
    datastore_->switch_epoch(116);

    // --- Step 2: Shutdown ---
    datastore_->shutdown();
    datastore_ = nullptr;

    // --- Step 3: Restart ---
    gen_datastore();

    // --- Step 4: Check after restart ---
    // Verify that available_boundary_version_ is properly restored after restart

    EXPECT_EQ(datastore_->get_available_boundary_version().get_major(), 0);
    EXPECT_EQ(datastore_->get_available_boundary_version().get_minor(), 0);

    // Verify data consistency
    auto cursor = datastore_->get_snapshot()->get_cursor();
    EXPECT_TRUE(cursor->next());

    std::string key;
    std::string value;
    cursor->key(key);
    cursor->value(value);
    EXPECT_EQ(key, "test_key");
    EXPECT_EQ(value, "test_value");

    EXPECT_FALSE(cursor->next());
}

TEST_F(datastore_blob_test, initial_available_boundary_version_) {
    auto boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 0);
    EXPECT_EQ(boundary_version.get_minor(), 0);

    // Change available_boundary_version_ and reboot
    datastore_->switch_epoch(1);
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1,1});
    lc0_->end_session();
    datastore_->switch_epoch(2);
    datastore_->switch_available_boundary_version({1, 5});

    boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 1);
    EXPECT_EQ(boundary_version.get_minor(), 5);

    datastore_->shutdown();
    datastore_.reset();
    gen_datastore();

    boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 0);
    EXPECT_EQ(boundary_version.get_minor(), 0);
}

TEST_F(datastore_blob_test, switch_available_boundary_version) {
    auto boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 0);
    EXPECT_EQ(boundary_version.get_minor(), 0);

    datastore_->switch_available_boundary_version({1, 5});
    boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 1);
    EXPECT_EQ(boundary_version.get_minor(), 5);

    datastore_->switch_available_boundary_version({1, 3});
    boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 1);
    EXPECT_EQ(boundary_version.get_minor(), 5);

    datastore_->switch_available_boundary_version({2, 3});
    boundary_version = datastore_->get_available_boundary_version();
    EXPECT_EQ(boundary_version.get_major(), 2);
    EXPECT_EQ(boundary_version.get_minor(), 3);
}

} // namespace limestone::testing