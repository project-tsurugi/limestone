#include <limestone/api/blob_file.h>
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>

namespace limestone::testing {

using namespace limestone::api;

TEST(blob_file_test, constructor_with_default_availability) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path);

    EXPECT_EQ(blob.path(), test_path);
    EXPECT_FALSE(static_cast<bool>(blob));
}

TEST(blob_file_test, constructor_with_availability) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path, true);

    EXPECT_EQ(blob.path(), test_path);
    EXPECT_TRUE(static_cast<bool>(blob));
}

TEST(blob_file_test, path_returns_correct_value) {
    boost::filesystem::path test_path("/path/to/blob");
    blob_file blob(test_path);

    EXPECT_EQ(blob.path(), test_path);
}

} // namespace limestone::api
