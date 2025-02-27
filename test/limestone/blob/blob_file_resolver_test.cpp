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
            boost::filesystem::path(base_directory));
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
    dir_name << "dir_" << std::setw(2) << std::setfill('0') << (blob_id % 100); // Mod 10 for directory count
    boost::filesystem::path expected_path = boost::filesystem::path(base_directory) / "blob" / dir_name.str();
    expected_path /= "000000000001e240.blob"; // Blob ID in hex: 123456 = 1e240

    ASSERT_EQ(path, expected_path);
}

TEST_F(blob_file_resolver_test, handles_multiple_blob_ids) {
    // Test multiple blob IDs resolve to correct paths
    for (blob_id_type blob_id = 0; blob_id < 100; ++blob_id) {
        auto path = resolver_->resolve_path(blob_id);

        std::ostringstream dir_name;
        dir_name << "dir_" << std::setw(2) << std::setfill('0') << (blob_id % 100); // Mod 10 for directory count
        boost::filesystem::path expected_path = boost::filesystem::path(base_directory) / "blob" / dir_name.str();
        std::ostringstream file_name;
        file_name << std::hex << std::setw(16) << std::setfill('0') << blob_id << ".blob";
        expected_path /= file_name.str();

        ASSERT_EQ(path, expected_path);
    }
}

// Test for get_blob_root()
TEST_F(blob_file_resolver_test, get_blob_root_returns_correct_directory) {
    boost::filesystem::path expected_root = boost::filesystem::path(base_directory) / "blob";
    EXPECT_EQ(resolver_->get_blob_root(), expected_root);
}

// Test for is_blob_file() with valid file names.
TEST_F(blob_file_resolver_test, is_blob_file_returns_true_for_valid_filename) {
    // Example of a valid filename: 16-digit hexadecimal + ".blob"
    std::string valid_filename = "000000000001e240.blob"; // blob_id 123456
    boost::filesystem::path valid_path = boost::filesystem::path("/some/path") / valid_filename;
    EXPECT_TRUE(resolver_->is_blob_file(valid_path));
}

// Test for is_blob_file() with invalid file names.
TEST_F(blob_file_resolver_test, is_blob_file_returns_false_for_invalid_filename) {
    // Insufficient digits
    boost::filesystem::path invalid_path1 = boost::filesystem::path("/some/path") / "0001e240.blob";
    EXPECT_FALSE(resolver_->is_blob_file(invalid_path1));

    // Different extension
    boost::filesystem::path invalid_path2 = boost::filesystem::path("/some/path") / "000000000001e240.data";
    EXPECT_FALSE(resolver_->is_blob_file(invalid_path2));

    // Contains non-hexadecimal character
    boost::filesystem::path invalid_path3 = boost::filesystem::path("/some/path") / "000000000001e24G.blob";
    EXPECT_FALSE(resolver_->is_blob_file(invalid_path3));
}

// Test for extract_blob_id()
TEST_F(blob_file_resolver_test, extract_blob_id_returns_correct_id) {
    // Example of a valid filename
    std::string filename = "000000000001e240.blob"; // blob_id 123456 = 0x1e240
    boost::filesystem::path file_path = boost::filesystem::path("/some/path") / filename;
    blob_id_type extracted = resolver_->extract_blob_id(file_path);
    blob_id_type expected = 123456; // 0x1e240
    EXPECT_EQ(extracted, expected);
}

}  // namespace limestone::testing
