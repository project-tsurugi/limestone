/*
 * Copyright 2022-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <sys/stat.h>  

#include <boost/filesystem.hpp>

#include <limestone/logging.h>
#include <limestone/api/datastore.h>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"

#include "test_root.h"

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;
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
        boost::filesystem::remove_all(location);
    }

    static bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    static bool is_pwal(const boost::filesystem::path& p) { return starts_with(p.filename().string(), "pwal"); }
    static void ignore_entry(limestone::api::log_entry&) {}

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
            // std::cerr << "Stacktrace: " << std::endl << boost::stacktrace::stacktrace() << std::endl;

            compaction_completed.store(true);
            if (switch_epoch_thread.joinable()) {
                switch_epoch_thread.join();
            }
            throw;  // Rethrow the exception to be handled by the caller
        } catch (...) {
            std::cerr << "Unknown exception caught" << std::endl;
            // std::cerr << "Stacktrace: " << std::endl << boost::stacktrace::stacktrace() << std::endl;

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
            if (file.rfind(prefix, 0) == 0) {
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

    void create_mainfest_file(int persistent_format_version = 1) {
        create_file(manifest_path, data_manifest(persistent_format_version));
        if (persistent_format_version > 1) {
            compaction_catalog catalog{location};
            catalog.update_catalog_file(0, {}, {});
        }
    }

    std::set<std::string> extract_pwal_files_from_datastore() {
        std::set<boost::filesystem::path> files = datastore_->files();

        std::set<std::string> pwal_file_names;
        for (const auto& file : files) {
            std::string filename = file.filename().string();
            if (filename.find("pwal") == 0) {  
                pwal_file_names.insert(filename);
            }
        }

        return pwal_file_names;
    }
};

TEST_F(online_compaction_test, no_pwals) {
    gen_datastore();
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location); 
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);   

    datastore_->switch_epoch(1);
    run_compact_with_epoch_switch(2);

    // No PWALs are present, so the catalog should not be updated.
    catalog = compaction_catalog::from_catalog_file(location); 
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);   
    pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());
}

TEST_F(online_compaction_test, scenario01) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // First compaction.
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2); // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2); // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Compaction run without any changes to PWALs.
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2); // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2); // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Remove detached PWALs to ensure that only compacted files are read.
    [[maybe_unused]] int result = std::system(("rm " + std::string(location) + "/pwal_000?.0*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2); // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    restart_datastore_and_read_snapshot();
    
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    run_compact_with_epoch_switch(4);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 2);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v1");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v3");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    // Add a new PWALs.
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v11", {3, 4});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v12", {3, 4});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k3", "v13", {3, 4});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    run_compact_with_epoch_switch(5);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Delete some detached PWALs.
    [[maybe_unused]] int result2 = std::system(("rm " + std::string(location) + "/pwal_000[12].*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v13");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    
    // Some PWALs are newly created.
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v23", {5, 0});
    lc0_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);

    // Reboot without rotation.
    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);

    // Rotate without any data changes.
    run_compact_with_epoch_switch(6);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 2);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    kv_list = restart_datastore_and_read_snapshot();
    ASSERT_EQ(kv_list.size(), 3);
    EXPECT_EQ(kv_list[0].first, "k1");
    EXPECT_EQ(kv_list[0].second, "v11");
    EXPECT_EQ(kv_list[1].first, "k2");
    EXPECT_EQ(kv_list[1].second, "v12");
    EXPECT_EQ(kv_list[2].first, "k3");
    EXPECT_EQ(kv_list[2].second, "v23");

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    // Some PWALs are newly created or updated.
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "k4", "v33", {6, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {6, 0});
    lc1_->end_session();


    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // Rotate.
    run_compact_with_epoch_switch(8);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 7); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);


    // Some PWALs are newly created.
    
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {8, 0});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k2", "v43", {8, 0});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // Rotate without reboot.
    run_compact_with_epoch_switch(9);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 8); 
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 6);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

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

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
}


// This test case, scenario02, is a modified version of scenario01.
// In scenario02, all calls to `restart_datastore_and_read_snapshot()`
// and checks on the returned kv_list values have been removed. By
// removing these calls and checks, the test focuses on verifying the
// behavior of compaction and file management without restarting the
// datastore or checking key-value data consistency. Since the datastore
// is not restarted, the timing of when the set of PWAL files maintained
// by the datastore is updated differs from scenario01, and therefore the
// test expectations have been changed.
TEST_F(online_compaction_test, scenario02) {
    gen_datastore();
    datastore_->switch_epoch(1);
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_TRUE(pwals.empty());

    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v3", {1, 0});
    lc1_->end_session();

    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_EQ(catalog.get_compacted_files().size(), 0);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 0);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 2);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // First compaction.
    run_compact_with_epoch_switch(2);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Compaction run without any changes to PWALs.
    run_compact_with_epoch_switch(3);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  // pwal_0000.xxx and pwal_0000.compacted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Remove detached PWALs to ensure that only compacted files are read.
    [[maybe_unused]] int result = std::system(("rm " + std::string(location) + "/pwal_000?.0*").c_str());

    pwals = extract_pwal_files_from_datastore();  
    EXPECT_EQ(pwals.size(), 3); // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    run_compact_with_epoch_switch(4);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 1);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    EXPECT_EQ(pwals.size(), 3); // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Add new PWALs.
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v11", {3, 4});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k2", "v12", {3, 4});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k3", "v13", {3, 4});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 6); // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);  
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    run_compact_with_epoch_switch(5);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 4);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4); // Detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Delete some detached PWALs.
    [[maybe_unused]] int result2 = std::system(("rm " + std::string(location) + "/pwal_000[12].*").c_str());

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4); // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

        // Some PWALs are newly created.
    lc0_->begin_session();
    lc0_->add_entry(1, "k3", "v23", {5, 0});
    lc0_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);  // Not yet detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    // Rotate.
    run_compact_with_epoch_switch(6);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 5);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 2);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);  // Detected that it has been deleted
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);

    // Some PWALs are newly created or updated.
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "k4", "v33", {6, 0});
    lc0_->end_session();
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {6, 0});
    lc1_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");

    // Rotate.
    run_compact_with_epoch_switch(8);

    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 7);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 5);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);

    // Some PWALs are newly created.
    lc1_->begin_session();
    lc1_->add_entry(1, "k1", "v33", {8, 0});
    lc1_->end_session();
    lc2_->begin_session();
    lc2_->add_entry(1, "k2", "v43", {8, 0});
    lc2_->end_session();

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    // Rotate.
    run_compact_with_epoch_switch(9);
    catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 8);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    ASSERT_PRED_FORMAT3(ContainsCompactedFileInfo, catalog.get_compacted_files(), compacted_filename, 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 6);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0000.", 3);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, catalog.get_detached_pwals(), "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 4);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);
}

TEST_F(online_compaction_test, safe_rename_success) {
    boost::filesystem::path from = boost::filesystem::path(location) / "test_file.txt";
    boost::filesystem::path to = boost::filesystem::path(location) / "renamed_file.txt";
    
    boost::filesystem::ofstream ofs(from);
    ofs << "test content";
    ofs.close();
    
    ASSERT_NO_THROW(safe_rename(from, to));
    
    ASSERT_TRUE(boost::filesystem::exists(to));

    boost::filesystem::remove(to);
}

TEST_F(online_compaction_test, safe_rename_throws_exception) {
    boost::filesystem::path from = boost::filesystem::path(location) / "non_existent_file.txt";
    boost::filesystem::path to = boost::filesystem::path(location) / "renamed_file.txt";
    
    ASSERT_THROW(safe_rename(from, to), std::runtime_error);
}

TEST_F(online_compaction_test, select_files_for_compaction) {
    std::set<boost::filesystem::path> rotation_end_files = {
        boost::filesystem::path(location) / "pwal_0001.0123456",
        boost::filesystem::path(location) / "pwal_0002.0123456",
        boost::filesystem::path(location) / "pwal_0003",
        boost::filesystem::path(location) / "other_file"
    };
    std::set<std::string> detached_pwals = {"pwal_0002.0123456"};
    std::set<std::string> expected = {"pwal_0001.0123456"};
    
    std::set<std::string> result = select_files_for_compaction(rotation_end_files, detached_pwals);
    ASSERT_EQ(result, expected);
}


TEST_F(online_compaction_test, ensure_directory_exists_directory_exists) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "test_dir";
    boost::filesystem::create_directory(dir);
    
    ASSERT_NO_THROW(ensure_directory_exists(dir));
}

TEST_F(online_compaction_test, ensure_directory_exists_directory_created) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "test_dir";
    
    ASSERT_NO_THROW(ensure_directory_exists(dir));
    ASSERT_TRUE(boost::filesystem::exists(dir));
}

TEST_F(online_compaction_test, ensure_directory_exists_throws_exception) {
    boost::filesystem::path file = boost::filesystem::path(location) / "test_file.txt";
    
    boost::filesystem::ofstream ofs(file);
    ofs.close();
    
    ASSERT_THROW(ensure_directory_exists(file), std::runtime_error);
}

TEST_F(online_compaction_test, ensure_directory_exists_parent_directory_missing) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "nonexistent_parent/test_dir";
    ASSERT_THROW(ensure_directory_exists(dir), std::runtime_error);
}


TEST_F(online_compaction_test, handle_existing_compacted_file_no_existing_files) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    
    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
}

TEST_F(online_compaction_test, handle_existing_compacted_file_with_existing_file) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::ofstream ofs(compacted_file);
    ofs.close();
    
    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
    ASSERT_TRUE(boost::filesystem::exists(location_path / "pwal_0000.compacted.prev"));
}

TEST_F(online_compaction_test, handle_existing_compacted_file_throws_exception) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::path compacted_prev_file = location_path / "pwal_0000.compacted.prev";
    boost::filesystem::ofstream ofs1(compacted_file);
    ofs1.close();
    boost::filesystem::ofstream ofs2(compacted_prev_file);
    ofs2.close();
    
    ASSERT_THROW(handle_existing_compacted_file(location_path), std::runtime_error);
}

TEST_F(online_compaction_test, get_files_in_directory) {
    boost::filesystem::path test_dir = boost::filesystem::path(location);
    boost::filesystem::path file1 = test_dir / "file1.txt";
    boost::filesystem::path file2 = test_dir / "file2.txt";
    boost::filesystem::ofstream ofs1(file1); 
    ofs1.close();
    boost::filesystem::ofstream ofs2(file2);
    ofs2.close();

    std::set<std::string> expected_files = { "file1.txt", "file2.txt" };

    std::set<std::string> files = get_files_in_directory(test_dir);
    EXPECT_EQ(files, expected_files);
}

TEST_F(online_compaction_test, get_files_in_directory_directory_not_exists) {
    boost::filesystem::path non_existent_dir = boost::filesystem::path(location) / "non_existent_dir";
    ASSERT_THROW(get_files_in_directory(non_existent_dir), std::runtime_error);
}

TEST_F(online_compaction_test, get_files_in_directory_not_a_directory) {
    boost::filesystem::path file_path = boost::filesystem::path(location) / "test_file.txt";
    boost::filesystem::ofstream ofs(file_path);
    ofs.close();

    ASSERT_THROW(get_files_in_directory(file_path), std::runtime_error);
}

TEST_F(online_compaction_test, get_files_in_directory_with_files) {
    boost::filesystem::path test_dir = boost::filesystem::path(location) / "test_dir";
    boost::filesystem::create_directory(test_dir);

    boost::filesystem::path file1 = test_dir / "file1.txt";
    boost::filesystem::path file2 = test_dir / "file2.txt";
    boost::filesystem::ofstream ofs1(file1);
    boost::filesystem::ofstream ofs2(file2);

    std::set<std::string> expected_files = {"file1.txt", "file2.txt"};
    std::set<std::string> files = get_files_in_directory(test_dir);

    EXPECT_EQ(files, expected_files);
}

TEST_F(online_compaction_test, get_files_in_directory_empty_directory) {
    boost::filesystem::path empty_dir = boost::filesystem::path(location) / "empty_test_dir";
    boost::filesystem::create_directory(empty_dir);

    std::set<std::string> files = get_files_in_directory(empty_dir);
    EXPECT_TRUE(files.empty());
}


TEST_F(online_compaction_test, remove_file_safely_success) {
    boost::filesystem::path file = boost::filesystem::path(location) / "test_file_to_remove.txt";

    {
        boost::filesystem::ofstream ofs(file);
        ofs << "test content";
    }

    ASSERT_TRUE(boost::filesystem::exists(file));
    ASSERT_NO_THROW(remove_file_safely(file));
    ASSERT_FALSE(boost::filesystem::exists(file));
}

TEST_F(online_compaction_test, remove_file_safely_no_exception_for_nonexistent_file) {
    boost::filesystem::path file = boost::filesystem::path(location) / "non_existent_file.txt";
    ASSERT_NO_THROW(remove_file_safely(file));
}


// This test is disabled because it is environment-dependent and may not work properly in CI environments.
TEST_F(online_compaction_test, DISABLED_remove_file_safely_fails_to_remove_file) {
    boost::filesystem::path test_dir = boost::filesystem::path(location);
    boost::filesystem::path file = test_dir / "protected_file.txt";

    boost::filesystem::ofstream ofs(file);
    ofs << "This file is protected and cannot be removed.";
    ofs.close();
    chmod(test_dir.string().c_str(), 0444); 

    ASSERT_THROW(remove_file_safely(file), std::runtime_error);

    chmod(test_dir.string().c_str(), 0755);
    boost::filesystem::remove(file);
}



} // namespace limestone::testing


