#include <gtest/gtest.h>
#include <limestone/api/datastore.h>
#include <limestone/api/blob_pool.h>
#include <limestone/api/blob_file.h>
#include <limits>
#include <string>
#include <memory>
#include <boost/filesystem.hpp>
#include "test_root.h"

namespace limestone::testing {

using limestone::api::blob_id_type;

constexpr const char* data_location = "/tmp/datastore_blob_test/data_location";
constexpr const char* metadata_location = "/tmp/datastore_blob_test/metadata_location";

class datastore_blob_test : public ::testing::Test {
protected:
    std::unique_ptr<api::datastore_test> datastore_;
    boost::filesystem::path location_;

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

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() override {
        if (datastore_) {
            datastore_->shutdown();
        }
        boost::filesystem::remove_all(location_);
    }

    void create_dummy_file(api::blob_id_type existing_blob_id) {
        auto file = datastore_->get_blob_file(existing_blob_id);
        auto path = file.path();
        boost::filesystem::create_directories(path.parent_path());
        boost::filesystem::ofstream dummy_file(path);
        dummy_file << "test data";
        dummy_file.close();
    }
};

TEST_F(datastore_blob_test, acquire_blob_pool_basic) {
    auto pool = datastore_->acquire_blob_pool();
    ASSERT_NE(pool, nullptr);
}


// Environment-independent part
TEST_F(datastore_blob_test, get_blob_file_basic) {
    int next_blob_id = 12345;
    int existing_blob_id = 12344;
    datastore_->set_next_blob_id(next_blob_id);

    create_dummy_file(existing_blob_id);
    create_dummy_file(next_blob_id);

    // Case 1: Normal case - file exists and is accessible
    auto file = datastore_->get_blob_file(existing_blob_id);
    EXPECT_TRUE(static_cast<bool>(file));

    // Case 2: File is removed after being confirmed to exist
    boost::filesystem::remove(file.path());
    auto file_removed = datastore_->get_blob_file(existing_blob_id);
    EXPECT_FALSE(static_cast<bool>(file_removed));

    // Case 3: Boundary condition - ID equal to next_blob_id
    auto file_next_blob_id = datastore_->get_blob_file(next_blob_id);
    EXPECT_TRUE(boost::filesystem::exists(file_next_blob_id.path()));
}

// Environment-dependent part (disabled in CI environment)
// Reason: This test modifies directory permissions to simulate a "permission denied" scenario.
// However, in certain environments, such as CI, the behavior might differ, making the test unreliable.
TEST_F(datastore_blob_test, DISABLED_get_blob_file_permission_error) {
    int existing_blob_id = 12344;
    create_dummy_file(existing_blob_id);

    auto file = datastore_->get_blob_file(existing_blob_id);

    // Simulate a permission denied scenario by modifying directory permissions
    boost::filesystem::permissions(file.path().parent_path(), boost::filesystem::perms::no_perms);
    EXPECT_THROW(boost::filesystem::exists(file.path()), boost::filesystem::filesystem_error);

    // Cleanup: Restore permissions for subsequent tests
    boost::filesystem::permissions(file.path().parent_path(), boost::filesystem::perms::all_all);
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
    auto *log_channel = &datastore_->create_channel(data_location);

    datastore_->ready();
    auto pool = datastore_->acquire_blob_pool();

    std::string data1 = "test data";
    std::string data2 = "more test data";
    auto blob_id1 = pool->register_data(data1);
    auto blob_id2 = pool->register_data(data2);

    auto blob_fil1 = datastore_->get_blob_file(blob_id1);
    auto blob_fil2 = datastore_->get_blob_file(blob_id2);

    EXPECT_TRUE(boost::filesystem::exists(blob_fil1.path()));
    EXPECT_TRUE(boost::filesystem::exists(blob_fil2.path()));

    log_channel->begin_session();
    log_channel->add_entry(1, "key1", "value1", {1,1}, {blob_id1});
    log_channel->end_session();

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



} // namespace limestone::testing