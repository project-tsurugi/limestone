
#include <boost/filesystem.hpp>

#include <limestone/logging.h>
#include <limestone/api/compaction_catalog.h>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

using namespace std::literals;
using dblog_scan = limestone::internal::dblog_scan;
using compaction_catalog = limestone::api::compaction_catalog;

namespace limestone::testing {

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern const std::string_view epoch_0_str;
extern const std::string_view epoch_0x100_str;
extern std::string data_manifest(int persistent_format_version = 1);
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;

class log_dir_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/log_dir_test";
const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);
const boost::filesystem::path compaction_catalog_path = boost::filesystem::path(location) / "compaction_catalog";

    void SetUp() {
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
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    static bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    static bool is_pwal(const boost::filesystem::path& p) { return starts_with(p.filename().string(), "pwal"); }
    static void ignore_entry(void*, limestone::api::log_entry&) {}

    void create_mainfest_file(int persistent_format_version = 1) {
        create_file(manifest_path, data_manifest(persistent_format_version));
        if (persistent_format_version > 1) {
            compaction_catalog catalog{location};
            catalog.update_catalog_file(0, {}, {});
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};


TEST_F(log_dir_test, newly_created_directory_contains_manifest_file) {
    gen_datastore();

    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
    EXPECT_TRUE(boost::filesystem::exists(compaction_catalog_path));
}

TEST_F(log_dir_test, reject_directory_without_manifest_file) {
    create_file(boost::filesystem::path(location) / "epoch", epoch_0_str);

    EXPECT_THROW({ gen_datastore(); }, std::exception);
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
    create_mainfest_file();

    gen_datastore();  // success
}

TEST_F(log_dir_test, accept_directory_only_correct_manifest_file) {
    create_mainfest_file();

    gen_datastore();  // success
}

TEST_F(log_dir_test, reject_directory_of_different_version) {
    create_mainfest_file(222);

    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, accept__manifest_version_v1) {
    create_mainfest_file(1);
    gen_datastore();   // success
}

TEST_F(log_dir_test, accept__manifest_version_v2) {
    create_mainfest_file(2);
    gen_datastore();   // success
}

TEST_F(log_dir_test, reject_manifest_version_v3) {
    create_mainfest_file(3);
    EXPECT_THROW({ gen_datastore(); }, std::exception);
}

TEST_F(log_dir_test, rotate_old_ok_v1_dir) {
    // setup backups
    boost::filesystem::path bk_path = boost::filesystem::path(location) / "bk";
    if (!boost::filesystem::create_directory(bk_path)) {
        LOG(FATAL) << "cannot make directory";
    }
    create_file(bk_path / "epoch", epoch_0_str);
    create_file(bk_path / std::string(limestone::internal::manifest_file_name), data_manifest(1));

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
    create_file(bk_path / std::string(limestone::internal::manifest_file_name), data_manifest(3));

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
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
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
    create_file(bk_path / std::string(limestone::internal::manifest_file_name), data_manifest(1));
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

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
    create_file(bk_path / std::string(limestone::internal::manifest_file_name), data_manifest(3));
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

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
    create_file(bk_path / std::string(limestone::internal::manifest_file_name),
                "{ \"answer\": 42 }");
    // setup entries
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("epoch", "epoch", false);
    entries.emplace_back(std::string(limestone::internal::manifest_file_name), std::string(limestone::internal::manifest_file_name), false);

    gen_datastore();

    EXPECT_EQ(datastore_->restore(bk_path.string(), entries), limestone::status::err_broken_data);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_normal) {
    create_mainfest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000", data_normal);

    // gen_datastore();
    // EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x100);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_EQ(ds.scan_pwal_files_throws(0x100, ignore_entry), 0x100);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_returns_max_epoch_nondurable) {
    create_mainfest_file();  // not used
    create_file(boost::filesystem::path(location) / "epoch", epoch_0x100_str);  // not used
    create_file(boost::filesystem::path(location) / "pwal_0000", data_nondurable);

    // gen_datastore();
    // EXPECT_EQ(limestone::internal::scan_pwal_files_in_dir(location, 2, is_pwal, 0x100, ignore_entry), 0x101);
    limestone::internal::dblog_scan ds{boost::filesystem::path(location)};
    ds.set_thread_num(2);
    EXPECT_EQ(ds.scan_pwal_files(0x100, ignore_entry, nullptr, [](limestone::api::log_entry::read_error&){return false;}), 0x101);
}

TEST_F(log_dir_test, scan_pwal_files_in_dir_rejects_unexpected_EOF) {
    create_mainfest_file();  // not used
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
    create_mainfest_file();  // not used
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
    create_mainfest_file();  // not used
    ASSERT_FALSE(boost::filesystem::is_empty(location));

    ASSERT_EQ(internal::purge_dir(location), status::ok);
    ASSERT_TRUE(boost::filesystem::is_empty(location));
}

/* check purge_dir returns err_permission_error: unimplemented.
   because creating the file that cannnot be deleted by test user requires super-user privileges or similar */
//TEST_F(log_dir_test, ut_purge_dir_err_file1) {}

}  // namespace limestone::testing
