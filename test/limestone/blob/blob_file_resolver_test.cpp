#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "blob_file_resolver.h"

namespace limestone::testing {

using limestone::api::blob_id_type;    

constexpr const char* base_directory = "/tmp/blob_file_resolver_test";

class blob_file_resolver_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        // Remove and recreate the test directory
        if (system(("rm -rf " + std::string(base_directory)).c_str()) != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system(("mkdir -p " + std::string(base_directory)).c_str()) != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        resolver_ = std::make_unique<limestone::internal::blob_file_resolver>(
            boost::filesystem::path(base_directory), 10 /* directory count */);
    }

    virtual void TearDown() {
        resolver_.reset();
        if (system(("rm -rf " + std::string(base_directory)).c_str()) != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<limestone::internal::blob_file_resolver> resolver_;
};

TEST_F(blob_file_resolver_test, resolves_correct_path) {
    // Test that paths are resolved correctly
    blob_id_type blob_id = 123456;

    auto path = resolver_->resolve_path(blob_id);

    // Expected path
    std::ostringstream dir_name;
    dir_name << "dir_" << std::setw(2) << std::setfill('0') << (blob_id % 10); // Mod 10 for directory count
    boost::filesystem::path expected_path = boost::filesystem::path(base_directory) / "blob" / dir_name.str();
    expected_path /= "000000000001e240.blob"; // Blob ID in hex: 123456 = 1e240

    ASSERT_EQ(path, expected_path);
}

TEST_F(blob_file_resolver_test, handles_multiple_blob_ids) {
    // Test multiple blob IDs resolve to correct paths
    for (blob_id_type blob_id = 0; blob_id < 100; ++blob_id) {
        auto path = resolver_->resolve_path(blob_id);

        std::ostringstream dir_name;
        dir_name << "dir_" << std::setw(2) << std::setfill('0') << (blob_id % 10); // Mod 10 for directory count
        boost::filesystem::path expected_path = boost::filesystem::path(base_directory) / "blob" / dir_name.str();
        std::ostringstream file_name;
        file_name << std::hex << std::setw(16) << std::setfill('0') << blob_id << ".blob";
        expected_path /= file_name.str();

        ASSERT_EQ(path, expected_path);
    }
}

}  // namespace limestone::testing
