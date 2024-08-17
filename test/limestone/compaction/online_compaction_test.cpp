
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/stacktrace.hpp>

#include <limestone/logging.h>
#include <limestone/api/compaction_catalog.h>
#include <limestone/api/datastore.h>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

using namespace std::literals;
using namespace limestone::api;

namespace limestone::testing {

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern const std::string_view epoch_0_str;
extern const std::string_view epoch_0x100_str;
extern std::string data_manifest(int persistent_format_version = 1);
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;

class online_compaction_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/online_compaction_test";
    const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);
    const boost::filesystem::path compaction_catalog_path = boost::filesystem::path(location) / "compaction_catalog";
    const std::string compacted_filename = compaction_catalog::get_compacted_filename();

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
        lc0_ = &datastore_->create_channel(location);
        lc1_ = &datastore_->create_channel(location);
        lc2_ = &datastore_->create_channel(location);

        datastore_->ready();
    }

    void TearDown() {
        datastore_ = nullptr;
        // boost::filesystem::remove_all(location);
    }

    static bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    static bool is_pwal(const boost::filesystem::path& p) { return starts_with(p.filename().string(), "pwal"); }
    static void ignore_entry(limestone::api::log_entry&) {}

    void create_mainfest_file(int persistent_format_version = 1) {
        create_file(manifest_path, data_manifest(persistent_format_version));
        if (persistent_format_version > 1) {
            compaction_catalog catalog{location};
            catalog.update_catalog_file(0, {}, {});
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    log_channel* lc0_{};
    log_channel* lc1_{};
    log_channel* lc2_{};

    void run_compact_with_epoch_switch(epoch_id_type epoch) {
        std::atomic<bool> compaction_completed(false);

        // Launch a separate thread to repeatedly call switch_epoch until the compaction is completed
        std::thread switch_epoch_thread([&]() {
            while (!compaction_completed.load()) {
                datastore_->switch_epoch(epoch);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        try {
            // Call compact_with_online in the main thread
            datastore_->compact_with_online();
        } catch (const std::exception& e) {
            std::cerr << "Exception caught: " << e.what() << std::endl;
            std::cerr << "Stacktrace: " << std::endl << boost::stacktrace::stacktrace() << std::endl;

            compaction_completed.store(true);
            if (switch_epoch_thread.joinable()) {
                switch_epoch_thread.join();
            }
            throw;  // Rethrow the exception to be handled by the caller
        } catch (...) {
            std::cerr << "Unknown exception caught" << std::endl;
            std::cerr << "Stacktrace: " << std::endl << boost::stacktrace::stacktrace() << std::endl;

            compaction_completed.store(true);
            if (switch_epoch_thread.joinable()) {
                switch_epoch_thread.join();
            }
            throw;  // Rethrow the exception to be handled by the caller
        }

        // Notify that the compaction is completed
        compaction_completed.store(true);
        if (switch_epoch_thread.joinable()) {
            switch_epoch_thread.join();
        }
    };

    std::vector<std::pair<std::string, std::string>> restart_datastore_and_read_snapshot() {
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();
        std::unique_ptr<snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<cursor> cursor = snapshot->get_cursor();

        std::vector<std::pair<std::string, std::string>> kv_list;

        while (cursor->next()) {
            std::string key, value;
            cursor->key(key);
            cursor->value(value);
            kv_list.emplace_back(key, value);
        }
        return kv_list;
    }
    
::testing::AssertionResult ContainsPrefix(const char* files_expr, const char* prefix_expr, const char* expected_count_expr,
                                          const std::set<std::string>& files, const std::string& prefix, int expected_count) {
    int match_count = 0;

    for (const auto& file : files) {
        if (file.rfind(prefix, 0) == 0) { // prefixで始まるかどうかをチェック
            match_count++;
        }
    }

    if (match_count == expected_count) {
        return ::testing::AssertionSuccess();
    } else {
        std::ostringstream oss;
        oss << files_expr << " which is " << ::testing::PrintToString(files)
            << ", contains " << match_count << " strings starting with " << prefix_expr
            << " which is \"" << prefix << "\", but expected " << expected_count_expr
            << " which is " << expected_count << ".";
        return ::testing::AssertionFailure() << oss.str();
    }
}


    ::testing::AssertionResult ContainsString(const char* files_expr, const char* target_expr,
                                          const std::set<std::string>& files, const std::string& target) {
    if (files.find(target) != files.end()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
        << files_expr << " (which is " << ::testing::PrintToString(files)
        << ") does not contain the string " << target_expr
        << " (which is \"" << target << "\").";
    }

    ::testing::AssertionResult ContainsCompactedFileInfo(const char* files_expr, const char* file_name_expr,
                                                        const char* version_expr, const std::set<compacted_file_info>& files,
                                                        const std::string& file_name, int version) {
        compacted_file_info target(file_name, version);
        if (files.find(target) != files.end()) {
            return ::testing::AssertionSuccess();
        }

        std::ostringstream oss;
        oss << files_expr << " which is\n{\n";
        for (const auto& file : files) {
            oss << "   {file_name: \"" << file.get_file_name() << "\", version: " << file.get_version() << "},\n";
        }
        oss << "}\ndoes not contain the specified compacted_file_info {file_name: \"" << file_name 
            << "\", version: " << version << "}.";

        return ::testing::AssertionFailure() << oss.str();
    }

};

TEST_F(online_compaction_test, no_pwals) {
    gen_datastore();
    
    compaction_catalog catalog =compaction_catalog::from_catalog_file(location); 
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 0);   

    datastore_->switch_epoch(1);
    run_compact_with_epoch_switch(2);

    // no pwals, catalog should not be updated
    catalog = compaction_catalog::from_catalog_file(location); 
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 0);   
}

TEST_F(online_compaction_test, scenario01) {
    gen_datastore();
    datastore_->switch_epoch(1);

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 0);

    // first compaction
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 2);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 1);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 1);

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    // fompaction without no pwal changed
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 2);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 1);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    // remove migraded pwals and only compacted files be read
    std::system(("rm " + std::string(location) + "/pwal_000?.0*").c_str());
    kv_list = restart_datastore_and_read_snapshot();

    run_compact_with_epoch_switch(4);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 2);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 1);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    // add a new pwal
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v11", {3, 4});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v12", {3, 4});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k3", "v13", {3, 4});
    lc2_->end_session();

    run_compact_with_epoch_switch(5);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 3);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 1);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 1);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    // delete some migrated pwals
    std::system(("rm " + std::string(location) + "/pwal_000[12].*").c_str());

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    // some pwals are newly created
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v23", {5, 0});
    lc0_->end_session();

    // reboot without rotation
    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    // rotate and no data changed
    run_compact_with_epoch_switch(6);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 2);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 2);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    // some pwals are newly create or update
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "k4", "v33", {6, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {6, 0});
    lc1_->end_session();

    // rotate 
    run_compact_with_epoch_switch(8);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 7); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 4);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 3);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 1);


    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {8, 0});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k2", "v43", {8, 0});
    lc2_->end_session();

    // rotate witoout reboot

    run_compact_with_epoch_switch(9);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 8); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_migrated_pwals().size(), 6);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0000.", 3);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0001.", 2);
    EXPECT_PRED_FORMAT3(ContainsPrefix, catalog.get_migrated_pwals(), "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v33");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v43");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");
    EXPECT_EQ(kv_list[3].first, "k4");
    EXPECT_EQ(kv_list[3].second, "v33");
 }

} // namespace limestone::testing