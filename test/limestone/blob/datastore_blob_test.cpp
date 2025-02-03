#include <gtest/gtest.h>
#include <limestone/api/datastore.h>
#include <limestone/api/blob_file.h>
#include <limits>
#include <string>
#include <memory>
#include <boost/filesystem.hpp>
#include "test_root.h"

namespace limestone::testing {

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



} // namespace limestone::testing