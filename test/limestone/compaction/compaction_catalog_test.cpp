// tests/compaction_catalog_test.cpp

#include <gtest/gtest.h>
#include "compaction_catalog.h"
#include <boost/filesystem.hpp>

// Namespace for convenience
using namespace limestone::api;

// Fixture for compaction_catalog tests
class compaction_catalog_test : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing
        temp_directory = boost::filesystem::temp_directory_path() / boost::filesystem::unique_path();
        boost::filesystem::create_directory(temp_directory);

        // Initialize the compaction catalog
        catalog = compaction_catalog::from_catalog_file(temp_directory);
    }

    void TearDown() override {
        // Remove the temporary directory and all its contents
        boost::filesystem::remove_all(temp_directory);
    }

    boost::filesystem::path temp_directory;
    compaction_catalog catalog;
};

// Test default constructor
TEST_F(compaction_catalog_test, DefaultConstructor) {
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());
    EXPECT_TRUE(catalog.get_migrated_pwals().empty());
}

// Test update_catalog method
TEST_F(compaction_catalog_test, UpdateCatalog) {
    epoch_id_type new_epoch_id = 42;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> migrated_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog(new_epoch_id, compacted_files, migrated_pwals);

    EXPECT_EQ(catalog.get_max_epoch_id(), new_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_migrated_pwals(), migrated_pwals);
}

// Test update_catalog_file method
TEST_F(compaction_catalog_test, UpdateCatalogFile) {
    epoch_id_type new_epoch_id = 42;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> migrated_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog(new_epoch_id, compacted_files, migrated_pwals);
    catalog.update_catalog_file();

    // Reload the catalog from the file
    compaction_catalog reloaded_catalog = compaction_catalog::from_catalog_file(temp_directory);

    EXPECT_EQ(reloaded_catalog.get_max_epoch_id(), new_epoch_id);
    EXPECT_EQ(reloaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(reloaded_catalog.get_migrated_pwals(), migrated_pwals);
}

