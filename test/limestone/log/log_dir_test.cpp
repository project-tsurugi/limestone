#include <limestone/logging.h>

#include <boost/filesystem.hpp>
#include <nlohmann/json.hpp>

#include "compaction_catalog.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "manifest.h"
#include "test_root.h"
#include "limestone/log/testdata.h"
#include "wal_sync/wal_history.h"

using namespace std::literals;
using dblog_scan = limestone::internal::dblog_scan;
using compaction_catalog = limestone::internal::compaction_catalog;
using limestone::internal::wal_history;
namespace limestone::testing {


class log_dir_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/log_dir_test";
const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest::file_name);
const boost::filesystem::path compaction_catalog_path = boost::filesystem::path(location) / "compaction_catalog";
const boost::filesystem::path wal_history_path = boost::filesystem::path(location) / "wal_history";

    void SetUp() {
        limestone::testing::enable_exception_throwing = true;
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() {
        limestone::testing::enable_exception_throwing = false;
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    static bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    static bool is_pwal(const boost::filesystem::path& p) { return starts_with(p.filename().string(), "pwal"); }
    static void ignore_entry(limestone::api::log_entry&) {}

    void create_manifest_file(int persistent_format_version = 1) {
        create_file(manifest_path, data_manifest(persistent_format_version));
        if (persistent_format_version > 1) {
            compaction_catalog catalog{location};
            catalog.update_catalog_file(0, 0, {}, {});
        }
    }

    void check_newly_created_wal_history(std::int64_t start) {
        wal_history wal_history_{boost::filesystem::path(location)};
        auto list = wal_history_.list();
        EXPECT_EQ(list.size(), 1);
        auto& record = list.front();
        EXPECT_EQ(record.epoch, 0);
        using namespace std::chrono;
        auto now = static_cast<std::int64_t>(std::time(nullptr));
        EXPECT_LE(record.timestamp, now);
        EXPECT_GE(record.timestamp, start);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};


TEST_F(log_dir_test, newly_created_directory) {
    auto start = static_cast<std::int64_t>(std::time(nullptr));
    gen_datastore();

    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
    EXPECT_TRUE(boost::filesystem::exists(compaction_catalog_path));

    datastore_->ready();
    check_newly_created_wal_history(start);
}

TEST_F(log_dir_test, reject_directory_without_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);

    try {
        gen_datastore();
        FAIL() << "Expected exception not thrown";
    } catch (const std::exception& e) {
        std::string what_msg = e.what();
        EXPECT_NE(what_msg.find("unsupported dbdir persistent format version:"), std::string::npos);
    }
}

TEST_F(log_dir_test, reject_directory_with_broken_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_file(manifest_path, "broken");

    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_only_broken_manifest_file) {
    create_file(manifest_path, "broken");

    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, reject_directory_only_broken_manifest_file2) {
    create_file(manifest_path, "{ \"answer\": 42 }");

    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, accept_directory_with_correct_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_manifest_file();

    gen_datastore();  // success
}

TEST_F(log_dir_test, accept_directory_only_correct_manifest_file) {
    create_manifest_file();

    auto start = static_cast<std::int64_t>(std::time(nullptr));
    gen_datastore(); 
    datastore_->ready();
    check_newly_created_wal_history(start);

}

TEST_F(log_dir_test, reject_directory_of_different_version) {
    create_manifest_file(222);

    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, accept_manifest_version_v1) {
    create_manifest_file(1);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept_manifest_version_v2) {
    create_manifest_file(2);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept_manifest_version_v3) {
    create_manifest_file(3);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept_manifest_version_v4) {
    create_manifest_file(4);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept_manifest_version_v5) {
    create_manifest_file(5);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept_manifest_version_v6) {
    create_manifest_file(6);
    gen_datastore();   // success
}

TEST_F(log_dir_test, log_dir_test_accept_manifest_version_v7) {
    create_manifest_file(7);
    gen_datastore();   // success
}

TEST_F(log_dir_test, reject_manifest_version_v8) {
    create_manifest_file(8);
    EXPECT_THROW({ gen_datastore(); }, std::exception);
}


TEST_F(log_dir_test, rotate_old_ok_v1_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name), data_manifest(1));

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::ok);
}

TEST_F(log_dir_test, rotate_old_rejects_unsupported_data) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name), data_manifest(8));

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_old_rejects_v0_logdir_missing_manifest) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_old_rejects_corrupted_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name),
                "{ \"answer\": 42 }");

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), true), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_ok_v1_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name), data_manifest(1));
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest::file_name), std::string(limestone::internal::manifest::file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::ok);
}

TEST_F(log_dir_test, rotate_prusik_rejects_unsupported_data) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name), data_manifest(8));
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest::file_name), std::string(limestone::internal::manifest::file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_rejects_v0_logdir_missing_manifest) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, rotate_prusik_rejects_corrupted_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest::file_name),
                "{ \"answer\": 42 }");
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest::file_name), std::string(limestone::internal::manifest::file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_normal) {
    create_manifest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000", data_normal);

    // gen_datastore();
    // EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x100);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_EQ(ds.scan_pwal_files_throws(0x100, ignore_entry), 0x100);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_nondurable) {
    create_manifest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000", data_nondurable);

    // gen_datastore();
    // EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x101);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_EQ(ds.scan_pwal_files(0x100, ignore_entry, [](limestone::api::log_entry::read_error&){return false;}), 0x101);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_rejects_unexpected_EOF) {
    create_manifest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x02\x01\x01\x00\x00\x00"
                ""sv);

    // gen_datastore();
    // EXPECT_THROW({
    //     limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry);
    // }, std::exception);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_THROW({
        ds.scan_pwal_files_throws(0x100, ignore_entry);
    }, std::exception);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_rejects_unexpeced_zeros) {
    create_manifest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000",
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"
                // XXX: epoch footer...
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                ""sv);

    // gen_datastore();
    // EXPECT_THROW({
    //     limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry);
    // }, std::exception);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_THROW({
        ds.scan_pwal_files_throws(0x100, ignore_entry);
    }, std::exception);
}

TEST_F(log_dir_test, ut_purge_dir_ok_file1) {
    create_manifest_file();  // not used
    ASSERT_FALSE(boost::filesystem::is_empty(location));

    ASSERT_EQ(internal::purge_dir(location), status::ok);
    ASSERT_TRUE(boost::filesystem::is_empty(location));
}

TEST_F(log_dir_test, setup_initial_logdir_creates_manifest_file) {
    // Setup initial logdir
    limestone::internal::setup_initial_logdir(boost::filesystem::path(location));

    // Check that the manifest file is created
    EXPECT_TRUE(boost::filesystem::exists(manifest_path));

    // Read the manifest file and verify its contents
    std::ifstream manifest_file(manifest_path.string());
    ASSERT_TRUE(manifest_file.is_open());
    nlohmann::json manifest;
    manifest_file >> manifest;

    EXPECT_EQ(manifest["format_version"], "1.1");
    EXPECT_EQ(manifest["persistent_format_version"], 7);
}

TEST_F(log_dir_test, restore_skips_manifest_when_destination_has_one) {
    // initialize datastore first (it will create manifest in destination)
    gen_datastore();

    // prepare backup directory (bk) with a manifest
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }

    // backup manifest with persistent_format_version = 2
    boost::filesystem::copy_file(manifest_path, bk_path / std::string(limestone::internal::manifest::file_name));

    // overwrite the destination manifest to simulate an existing manifest with different content
    create_file(manifest_path, data_manifest(999));

    // run restore with purge_destination = false so destination manifest should not be overwritten
    auto rc = datastore_->restore(bk_path.string(), true, false);
    EXPECT_EQ(rc, limestone::status::ok);

    // verify destination manifest still contains persistent_format_version = 999
    std::ifstream f(manifest_path.string());
    ASSERT_TRUE(f.is_open());
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("999"), std::string::npos);
}

TEST_F(log_dir_test, setup_initial_logdir_creates_compaction_catalog_if_not_exists) {
    // Ensure that the compaction catalog does not exist before
    boost::filesystem::remove(compaction_catalog_path);

    // Setup initial logdir
    limestone::internal::setup_initial_logdir(boost::filesystem::path(location));

    // Check that the compaction catalog is created
    EXPECT_TRUE(boost::filesystem::exists(compaction_catalog_path));
}


TEST_F(log_dir_test, setup_initial_logdir_does_not_modify_existing_compaction_catalog) {
    // Create a dummy compaction catalog file to simulate pre-existing catalog
    create_file(compaction_catalog_path, "{}");

    // Save the current state of the compaction catalog
    std::ifstream initial_catalog(compaction_catalog_path.string());
    std::string initial_catalog_content((std::istreambuf_iterator<char>(initial_catalog)),
                                        std::istreambuf_iterator<char>());

    // Setup initial logdir again
    limestone::internal::setup_initial_logdir(boost::filesystem::path(location));

    // Verify that the compaction catalog has not been modified
    std::ifstream modified_catalog(compaction_catalog_path.string());
    std::string modified_catalog_content((std::istreambuf_iterator<char>(modified_catalog)),
                                         std::istreambuf_iterator<char>());
    EXPECT_EQ(initial_catalog_content, modified_catalog_content);
}

TEST_F(log_dir_test, exists_path_returns_true_for_existing_file) {
    // Create a file to test
    create_file(manifest_path, data_manifest());

    // Test that exists_path returns true for an existing file
    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
}

TEST_F(log_dir_test, exists_path_returns_false_for_non_existing_file) {
    // Test that exists_path returns false for a non-existing file
    EXPECT_FALSE(boost::filesystem::exists(manifest_path));
}



/* check purge_dir returns err_permission_error: unimplemented.
   because creating the file that cannnot be deleted by test user requires super-user privileges or similar */
//TEST_F(log_dir_test, ut_purge_dir_err_file1) {}

TEST_F(log_dir_test, ready_rotates_pwal_files_if_migration_info_requires_rotation) {
    // 1. Create valid manifest, epoch, and pwal files
    create_manifest_file(5); // Rotation required from 5 to 6
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_file(boost::filesystem::path(location) / "pwal_0000", data_normal);

    gen_datastore();

    // 3. Call ready()
    datastore_->ready();

    // 4. Verify the file name after rotation
    bool rotated = false;
    for (auto& entry : boost::filesystem::directory_iterator(location)) {
        if (entry.path().filename().string().find("pwal_0000.") == 0) {
            rotated = true;
        }
    }
    EXPECT_TRUE(rotated);
}

TEST_F(log_dir_test, ready_does_not_rotate_pwal_files_if_migration_info_does_not_require_rotation) {
    create_manifest_file(6); // No rotation required from 6 to 7
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);
    create_file(boost::filesystem::path(location) / "pwal_0000", data_normal);

    gen_datastore();

    datastore_->ready();

    // Verify that pwal_0000 remains unchanged
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000"));
}

}  // namespace limestone::testing
