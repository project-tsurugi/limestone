#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <limestone/api/compaction_catalog.h>

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::api::compacted_file_info;
using limestone::api::compaction_catalog;
class compaction_catalog_test : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir = boost::filesystem::temp_directory_path() / boost::filesystem::unique_path();
        boost::filesystem::create_directory(test_dir);

        catalog_file_path = test_dir / "compaction_catalog";
        backup_file_path = test_dir / "compaction_catalog.back";
    }

    void TearDown() override {
        boost::filesystem::remove_all(test_dir);
    }

    boost::filesystem::path test_dir;
    boost::filesystem::path catalog_file_path;
    boost::filesystem::path backup_file_path;
};

TEST_F(compaction_catalog_test, CreateCatalog) {
    compaction_catalog catalog(test_dir);

    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());
    EXPECT_TRUE(catalog.get_detached_pwals().empty());
}

TEST_F(compaction_catalog_test, UpdateCatalog) {
    compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);
}

TEST_F(compaction_catalog_test, UpdateAndLoadCatalogFile) {
    compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(loaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(loaded_catalog.get_detached_pwals(), detached_pwals);
}

TEST_F(compaction_catalog_test, LoadFromBackup) {
    {
        compaction_catalog catalog(test_dir);

        epoch_id_type max_epoch_id = 123;
        std::set<compacted_file_info> compacted_files = {
            {"file1", 1},
            {"file2", 2}
        };
        std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

        catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    }

    boost::filesystem::rename(catalog_file_path, backup_file_path);

    boost::filesystem::remove(catalog_file_path);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), 123);
    EXPECT_EQ(loaded_catalog.get_compacted_files().size(), 2);
    EXPECT_EQ(loaded_catalog.get_detached_pwals().size(), 2);
}

}  // namespace limestone::testing
