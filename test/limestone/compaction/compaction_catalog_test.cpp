#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "compaction_catalog.h"

namespace limestone::internal {

using limestone::api::epoch_id_type;
class compaction_catalog_test : public ::testing::Test {
protected:
    void SetUp() override {
        // テスト用のディレクトリを作成
        test_dir = boost::filesystem::temp_directory_path() / boost::filesystem::unique_path();
        boost::filesystem::create_directory(test_dir);

        // テスト用のファイルパスを設定
        catalog_file_path = test_dir / "compaction_catalog";
        backup_file_path = test_dir / "compaction_catalog.back";
    }

    void TearDown() override {
        // テスト用のディレクトリを削除
        boost::filesystem::remove_all(test_dir);
    }

    // テスト用のディレクトリパス
    boost::filesystem::path test_dir;
    boost::filesystem::path catalog_file_path;
    boost::filesystem::path backup_file_path;
};

TEST_F(compaction_catalog_test, CreateCatalog) {
    compaction_catalog catalog(test_dir);

    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());
    EXPECT_TRUE(catalog.get_migrated_pwals().empty());
}

TEST_F(compaction_catalog_test, UpdateCatalog) {
    compaction_catalog catalog(test_dir);

    limestone::internal::epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> migrated_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, migrated_pwals);

    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_migrated_pwals(), migrated_pwals);
}

TEST_F(compaction_catalog_test, UpdateAndLoadCatalogFile) {
    compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> migrated_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, migrated_pwals);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(loaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(loaded_catalog.get_migrated_pwals(), migrated_pwals);
}

TEST_F(compaction_catalog_test, LoadFromBackup) {
    // まずカタログファイルを作成
    {
        compaction_catalog catalog(test_dir);

        epoch_id_type max_epoch_id = 123;
        std::set<compacted_file_info> compacted_files = {
            {"file1", 1},
            {"file2", 2}
        };
        std::set<std::string> migrated_pwals = {"pwal1", "pwal2"};

        catalog.update_catalog_file(max_epoch_id, compacted_files, migrated_pwals);

    }

    // バックアップを作成
    boost::filesystem::rename(catalog_file_path, backup_file_path);

    // カタログファイルを削除
    boost::filesystem::remove(catalog_file_path);

    // バックアップから読み込み
    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), 123);
    EXPECT_EQ(loaded_catalog.get_compacted_files().size(), 2);
    EXPECT_EQ(loaded_catalog.get_migrated_pwals().size(), 2);
}

}  // namespace limestone::internal
