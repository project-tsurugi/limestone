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

class compaction_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/compaction_test";
    const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);
    const boost::filesystem::path compaction_catalog_path = boost::filesystem::path(location) / "compaction_catalog";
    const std::string compacted_filename = compaction_catalog::get_compacted_filename();

    void SetUp() {
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
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
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
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

    void run_compact_with_epoch_switch_org(epoch_id_type epoch) {
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

    void run_compact_with_epoch_switch(epoch_id_type epoch) {
        std::mutex wait_mutex;
        std::condition_variable wait_cv;
        bool wait_triggered = false;

        // Get the raw pointer from the unique_ptr
        auto* test_datastore = dynamic_cast<datastore_test*>(datastore_.get());
        if (test_datastore == nullptr) {
            throw std::runtime_error("datastore_ must be of type datastore_test");
        }

        // Set up the on_wait1 callback to signal when rotate_log_files() reaches the wait point
        test_datastore->on_wait1_callback = [&]() {
            std::unique_lock<std::mutex> lock(wait_mutex);
            wait_triggered = true;
            wait_cv.notify_one();  // Notify that on_wait1 has been triggered
        };

        try {
            // Run compact_with_online in a separate thread
            auto future = std::async(std::launch::async, [&]() { datastore_->compact_with_online(); });

            // Wait for on_wait1 to be triggered (simulating the waiting in rotate_log_files)
            {
                std::unique_lock<std::mutex> lock(wait_mutex);
                wait_cv.wait(lock, [&]() { return wait_triggered; });
            }

            // Now switch the epoch after on_wait1 has been triggered
            datastore_->switch_epoch(epoch);

            // Wait for the compact operation to finish
            future.get();  // Will rethrow any exception from compact_with_online
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            throw;  // Re-throw the exception for further handling
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

    void print_log_entry(const log_entry& entry) {
        std::string key;
        storage_id_type storage_id = entry.storage();
        log_entry::entry_type type = entry.type();

        if (type == log_entry::entry_type::normal_entry || type == log_entry::entry_type::remove_entry) {
            entry.key(key);
        }

        switch (type) {
            case log_entry::entry_type::normal_entry: {
                std::string value;
                entry.value(value);
                std::cout << "Entry Type: normal_entry, Storage ID: " << storage_id
                        << ", Key: " << key << ", Value: " << value
                        << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                        << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc())
                        << std::endl;
                break;
            }
            case log_entry::entry_type::remove_entry: {
                std::cout << "Entry Type: remove_entry, Storage ID: " << storage_id << ", Key: " << key
                        << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                        << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc())
                        << std::endl;
                break;
            }
            case log_entry::entry_type::clear_storage:
            case log_entry::entry_type::add_storage:
            case log_entry::entry_type::remove_storage: {
                write_version_type write_version;
                entry.write_version(write_version);
                std::cout << "Entry Type: " << static_cast<int>(type) << ", Storage ID: " << storage_id
                        << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                        << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc())
                        << std::endl;
                break;
            }
            case log_entry::entry_type::marker_begin:
                std::cout << "Entry Type: marker_begin, Epoch ID: " << entry.epoch_id() << std::endl;
                break;
            case log_entry::entry_type::marker_end:
                std::cout << "Entry Type: marker_end, Epoch ID: " << entry.epoch_id() << std::endl;
                break;
            case log_entry::entry_type::marker_durable:
                std::cout << "Entry Type: marker_durable, Epoch ID: " << entry.epoch_id() << std::endl;
                break;
            case log_entry::entry_type::marker_invalidated_begin:
                std::cout << "Entry Type: marker_invalidated_begin, Epoch ID: " << entry.epoch_id() << std::endl;
                break;
            default:
                std::cout << "Entry Type: unknown" << std::endl;
                break;
        }
    }




    /**
     * @brief Reads a specified log file (PWAL, compacted_file, snapshot) and returns a list of log entries.
     * @param log_file The path to the log file to be scanned.
     * @return A vector of log_entry objects read from the log file.
     */
    std::vector<log_entry> read_log_file(const std::string& log_file, const boost::filesystem::path& log_dir) {
        boost::filesystem::path log_path = log_dir / log_file;

        std::vector<log_entry> log_entries;
        limestone::internal::dblog_scan::parse_error pe;

        // Define a lambda function to capture and store log entries
        auto add_entry = [&](log_entry& e) { log_entries.push_back(e); };

        // Error reporting function, returning bool as expected by error_report_func_t
        auto report_error = [](log_entry::read_error& error) -> bool {
            std::cerr << "Error during log file scan: " << error.message() << std::endl;
            return false;  // Return false to indicate an error occurred
        };

        // Initialize a dblog_scan instance with the log directory
        limestone::internal::dblog_scan scanner(log_dir);

        // Scan the specified log file
        epoch_id_type max_epoch = scanner.scan_one_pwal_file(log_path, UINT64_MAX, add_entry, report_error, pe);

        if (pe.value() != limestone::internal::dblog_scan::parse_error::ok) {
            std::cerr << "Parse error occurred while reading the log file: " << log_path.string() << std::endl;
        }

        // Iterate over the log entries and print relevant information
        std::cout << std::endl << "Log entries read from " << log_path.string() << ":" << std::endl;
        for (const auto& entry : log_entries) {
            print_log_entry(entry); 
        }

        return log_entries;
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

    ::testing::AssertionResult AssertLogEntry(const log_entry& entry, const std::optional<storage_id_type>& expected_storage_id,
                                              const std::optional<std::string>& expected_key, const std::optional<std::string>& expected_value,
                                              const std::optional<epoch_id_type>& expected_epoch_number, const std::optional<std::uint64_t>& expected_minor_version, 
                                              log_entry::entry_type expected_type) {
        // Check the entry type
        if (entry.type() != expected_type) {
            return ::testing::AssertionFailure()
                << "Expected entry type: " << static_cast<int>(expected_type)
                << ", but got: " << static_cast<int>(entry.type());
        }

        // Check the storage ID if it exists
        if (expected_storage_id.has_value()) {
            if (entry.storage() != expected_storage_id.value()) {
                return ::testing::AssertionFailure()
                    << "Expected storage ID: " << expected_storage_id.value()
                    << ", but got: " << entry.storage();
            }
        }

        // Check the key if it exists
        if (expected_key.has_value()) {
            std::string actual_key;
            entry.key(actual_key);
            if (actual_key != expected_key.value()) {
                return ::testing::AssertionFailure()
                    << "Expected key: " << expected_key.value()
                    << ", but got: " << actual_key;
            }
        }

        // Check the value if it exists
        if (expected_value.has_value()) {
            std::string actual_value;
            entry.value(actual_value);
            if (actual_value != expected_value.value()) {
                return ::testing::AssertionFailure()
                    << "Expected value: " << expected_value.value()
                    << ", but got: " << actual_value;
            }
        }

        // Check the write version if it exists
        if (expected_epoch_number.has_value() && expected_minor_version.has_value()) {
            epoch_id_type actual_epoch_number = log_entry::write_version_epoch_number(entry.value_etc());
            std::uint64_t actual_minor_version = log_entry::write_version_minor_write_version(entry.value_etc());
            
            if (actual_epoch_number != expected_epoch_number.value() ||
                actual_minor_version != expected_minor_version.value()) {
                return ::testing::AssertionFailure()
                    << "Expected write version (epoch_number: " << expected_epoch_number.value()
                    << ", minor_write_version: " << expected_minor_version.value()
                    << "), but got (epoch_number: " << actual_epoch_number
                    << ", minor_write_version: " << actual_minor_version << ")";
            }
        }

        // If all checks pass, return success
        return ::testing::AssertionSuccess();
    }
};

TEST_F(compaction_test, no_pwals) {
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

TEST_F(compaction_test, scenario01) {
    FLAGS_v = 50;

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
    // EXPECT_EQ(catalog.get_max_epoch_id(), 0);
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
    EXPECT_EQ(catalog.get_max_epoch_id(), 1); 
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
TEST_F(compaction_test, scenario02) {
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

// This test case verifies the correct behavior of `remove_entry`.
TEST_F(compaction_test, scenario03) {
    FLAGS_v = 50;  // set VLOG level to 50

    // 1. Create multiple PWALs using two different storage IDs
    gen_datastore();
    datastore_->switch_epoch(1);

    // Storage ID 1: key1 added, then removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->remove_entry(1, "key1", {1, 1});  // use remove_entry
    lc0_->end_session();

    // Storage ID 2: key2 added, no removal
    lc1_->begin_session();
    lc1_->add_entry(2, "key2", "value2", {1, 0});
    lc1_->end_session();

    // Storage ID 1: key3 removed first, then added
    lc2_->begin_session();
    lc2_->remove_entry(1, "key3", {1, 0});
    lc2_->add_entry(1, "key3", "value3", {1, 3});
    lc2_->end_session();

    // Storeage ID 1: key4 deleted witout adding
    lc0_->begin_session();
    lc0_->remove_entry(1, "key4", {1, 0});
    lc0_->end_session();

    datastore_->switch_epoch(2);

    // Check the created PWAL files
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    auto log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key1", std::nullopt, 1, 1, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key4", std::nullopt, 1, 0, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 1);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key2", "value2", 1, 0, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", std::nullopt, 1, 0,log_entry ::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key3", "value3", 1, 3, log_entry::entry_type::normal_entry));

    // 2. Execute compaction
    run_compact_with_epoch_switch(3);

    // Check the catalog and PWALs after compaction
    compaction_catalog catalog = compaction_catalog::from_catalog_file(location);
    EXPECT_EQ(catalog.get_max_epoch_id(), 2);
    EXPECT_EQ(catalog.get_compacted_files().size(), 1);
    EXPECT_EQ(catalog.get_detached_pwals().size(), 3);

    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);  // Includes the compacted file
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");

    log_entries = read_log_file("pwal_0000.compacted", location);
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, log_entry::entry_type::normal_entry));  // write version changed to 0
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, log_entry::entry_type::normal_entry));  // write version changed to 0

    // 3. Add/Update PWALs (include remove_entry again)
    
    // Storage ID 1: key1 added, then removed
    lc0_->begin_session();
    lc0_->add_entry(1, "key11", "value1", {2, 0});
    lc0_->remove_entry(1, "key11", {2, 1});  // use remove_entry
    lc0_->end_session();

    // Storage ID 2: key2 added, no removal
    lc1_->begin_session();
    lc1_->add_entry(2, "key21", "value2", {2, 0});
    lc1_->end_session();

    // Storage ID 1: key3 removed first, then added
    lc2_->begin_session();
    lc2_->remove_entry(1, "key31", {2, 0});
    lc2_->add_entry(1, "key31", "value3", {2, 3});
    lc2_->end_session();

    // Storeage ID 1: key4 deleted witout adding
    lc0_->begin_session();
    lc0_->remove_entry(1, "key41", {2, 0});
    lc0_->end_session();

    datastore_->switch_epoch(4);
    pwals = extract_pwal_files_from_datastore();

    // Check the created PWAL files
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);  // 3 new pwals and 3 rotaed pwals and 1 compacted file
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0001");
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0002");

    log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 2, 0, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key11", std::nullopt, 2, 1, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, log_entry::entry_type::remove_entry));
    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 1);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key21", "value2", 2, 0, log_entry::entry_type::normal_entry));
    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key31", std::nullopt, 2, 0, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, log_entry::entry_type::normal_entry));

    // 4. Restart the datastore
    datastore_->shutdown();
    datastore_ = nullptr;
    gen_datastore();  // Restart

    // 5. check the compacted file and snapshot creating at the boot time
    log_entries = read_log_file("pwal_0000.compacted", location);
    ASSERT_EQ(log_entries.size(), 2);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key3", "value3", 0, 0, log_entry::entry_type::normal_entry));  // write version changed to 0
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key2", "value2", 0, 0, log_entry::entry_type::normal_entry));  // write version changed to 0

    log_entries = read_log_file("data/snapshot", location);
    ASSERT_EQ(log_entries.size(), 4);  // Ensure that there are log entries
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", std::nullopt, 2, 1, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key31", "value3", 2, 3, log_entry::entry_type::normal_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key41", std::nullopt, 2, 0, log_entry::entry_type::remove_entry));
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key21", "value2", 2, 0, log_entry::entry_type::normal_entry));

    // 5. Verify the snapshot contents after restart
    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();
    
    // key1 should exist with its updated value, key2 and key3 should be removed
    ASSERT_EQ(kv_list.size(), 4);
    EXPECT_EQ(kv_list[0].first, "key3");
    EXPECT_EQ(kv_list[0].second, "value3");
    EXPECT_EQ(kv_list[1].first, "key31");
    EXPECT_EQ(kv_list[1].second, "value3");
    EXPECT_EQ(kv_list[2].first, "key2");
    EXPECT_EQ(kv_list[2].second, "value2");
    EXPECT_EQ(kv_list[3].first, "key21");
    EXPECT_EQ(kv_list[3].second, "value2");
}

// This test case verifies the correct behavior of `remove_storage`.
// This test case verifies the correct behavior of `remove_storage`.
TEST_F(compaction_test, scenario04) {
    FLAGS_v = 50;  // set VLOG level to 50

    gen_datastore();
    datastore_->switch_epoch(1);

    // Storage ID 1: Add normal entries
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1, 0});
    lc0_->add_entry(1, "key2", "value2", {1, 1});
    lc0_->end_session();

    // Storage ID 2: Add normal entries
    lc1_->begin_session();
    lc1_->add_entry(2, "key3", "value3", {1, 0});
    lc1_->add_entry(2, "key4", "value4", {1, 1});
    lc1_->end_session();

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(1, "key5", "value5", {1, 2});
    lc2_->add_entry(1, "key6", "value6", {1, 3});
    lc2_->end_session();

    // Advance the epoch to 2
    datastore_->switch_epoch(2);

    // Remove storage for Storage ID 2
    lc1_->begin_session();
    lc1_->remove_storage(2, {2, 0});  // Removes the storage with ID 2
    lc1_->end_session();

    // Advance the epoch to 3
    datastore_->switch_epoch(3);

    // Add an entry to Storage ID 1
    lc0_->begin_session();
    lc0_->add_entry(1, "key7", "value7", {3, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2
    lc1_->begin_session();
    lc1_->add_entry(2, "key8", "value8", {3, 0});
    lc1_->end_session();

    // Chek PWALs before compaction
    auto pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 3);

    std::vector<log_entry> log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 1, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 1, 1, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key7", "value7", 3, 0, log_entry::entry_type::normal_entry)); 

    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 4);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key3", "value3", 1, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key4", "value4", 1, 1, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "", "", 2, 0, log_entry::entry_type::remove_storage)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key8", "value8", 3, 0, log_entry::entry_type::normal_entry)); 

    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key5", "value5", 1, 2, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key6", "value6", 1, 3, log_entry::entry_type::normal_entry)); 

    // online compaction
    run_compact_with_epoch_switch(4);

    // Check PWALs after compaction
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 4);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    log_entries = read_log_file("pwal_0000.compacted", location);
    ASSERT_EQ(log_entries.size(), 6);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, log_entry::entry_type::normal_entry)); 


    // Storage ID 1: Add normal entries
    lc0_->begin_session();
    lc0_->add_entry(1, "key11", "value1", {4, 0});
    lc0_->add_entry(1, "key12", "value2", {4, 1});
    lc0_->end_session();

    // Storage ID 2: Add normal entries
    lc1_->begin_session();
    lc1_->add_entry(2, "key13", "value3", {4, 0});
    lc1_->add_entry(2, "key14", "value4", {4, 1});
    lc1_->end_session();

    // Storage ID 1: Add more normal entries
    lc2_->begin_session();
    lc2_->add_entry(1, "key15", "value5", {4, 2});
    lc2_->add_entry(1, "key16", "value6", {4, 3});
    lc2_->end_session();

    // Advance the epoch to 5
    datastore_->switch_epoch(5);

    // Remove storage for Storage ID 1
    lc1_->begin_session();
    lc1_->remove_storage(1, {5, 0});  // Removes the storage with ID 2
    lc1_->end_session();

    // Advance the epoch to 6
    datastore_->switch_epoch(6);

    // Add an entry to Storage ID 1
    lc0_->begin_session();
    lc0_->add_entry(1, "key17", "value7", {6, 0});
    lc0_->end_session();

    // Add an entry to Storage ID 2
    lc1_->begin_session();
    lc1_->add_entry(2, "key18", "value8", {6, 0});
    lc1_->end_session();

    // Advance the epoch to 6
    datastore_->switch_epoch(7);

    // Chek newly created PWALs 
    pwals = extract_pwal_files_from_datastore();
    EXPECT_EQ(pwals.size(), 7);
    ASSERT_PRED_FORMAT2(ContainsString, pwals, "pwal_0000.compacted");
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0000.", 2);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0001.", 1);
    ASSERT_PRED_FORMAT3(ContainsPrefix, pwals, "pwal_0002.", 1);

    log_entries = read_log_file("pwal_0000", location);
    ASSERT_EQ(log_entries.size(), 3);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key11", "value1", 4, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key12", "value2", 4, 1, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key17", "value7", 6, 0, log_entry::entry_type::normal_entry)); 

    log_entries = read_log_file("pwal_0001", location);
    ASSERT_EQ(log_entries.size(), 4);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 2, "key13", "value3", 4, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key14", "value4", 4, 1, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "", "", 5, 0, log_entry::entry_type::remove_storage)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, log_entry::entry_type::normal_entry)); 

    log_entries = read_log_file("pwal_0002", location);
    ASSERT_EQ(log_entries.size(), 2);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key15", "value5", 4, 2, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key16", "value6", 4, 3, log_entry::entry_type::normal_entry)); 

    // Restart the datastore

    std::vector<std::pair<std::string, std::string>> kv_list = restart_datastore_and_read_snapshot();

    // check the compacted file and snapshot creating at the boot time
    log_entries = read_log_file("pwal_0000.compacted", location);
    ASSERT_EQ(log_entries.size(), 6);  
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key1", "value1", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 1, "key2", "value2", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 1, "key5", "value5", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 1, "key6", "value6", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[4], 1, "key7", "value7", 0, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[5], 2, "key8", "value8", 0, 0, log_entry::entry_type::normal_entry)); 

    log_entries = read_log_file("data/snapshot", location);
    ASSERT_EQ(log_entries.size(), 4);
    EXPECT_TRUE(AssertLogEntry(log_entries[0], 1, "key17", "value7", 6, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[1], 2, "key13", "value3", 4, 0, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[2], 2, "key14", "value4", 4, 1, log_entry::entry_type::normal_entry)); 
    EXPECT_TRUE(AssertLogEntry(log_entries[3], 2, "key18", "value8", 6, 0, log_entry::entry_type::normal_entry)); 

    // 5. Verify the snapshot contents after restart
    
    // key1 should exist with its updated value, key2 and key3 should be removed
    ASSERT_EQ(kv_list.size(), 5);
    EXPECT_EQ(kv_list[0].first, "key17");
    EXPECT_EQ(kv_list[0].second, "value7");
    EXPECT_EQ(kv_list[1].first, "key13");
    EXPECT_EQ(kv_list[1].second, "value3");
    EXPECT_EQ(kv_list[2].first, "key14");
    EXPECT_EQ(kv_list[2].second, "value4");
    EXPECT_EQ(kv_list[3].first, "key18");
    EXPECT_EQ(kv_list[3].second, "value8");
    EXPECT_EQ(kv_list[4].first, "key8");
    EXPECT_EQ(kv_list[4].second, "value8");
}





// This test is disabled because it is environment-dependent and may not work properly in CI environments.
TEST_F(compaction_test, DISABLED_fail_compact_with_io_error) {
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

    // remove the file to cause an I/O error
    [[maybe_unused]] int result = std::system(("chmod 0500 " + std::string(location)).c_str());

    // First compaction.
    ASSERT_THROW(run_compact_with_epoch_switch(2), limestone_exception);
}


TEST_F(compaction_test, safe_rename_success) {
    boost::filesystem::path from = boost::filesystem::path(location) / "test_file.txt";
    boost::filesystem::path to = boost::filesystem::path(location) / "renamed_file.txt";
    
    boost::filesystem::ofstream ofs(from);
    ofs << "test content";
    ofs.close();
    
    ASSERT_NO_THROW(safe_rename(from, to));
    
    ASSERT_TRUE(boost::filesystem::exists(to));

    boost::filesystem::remove(to);
}

TEST_F(compaction_test, safe_rename_throws_exception) {
    boost::filesystem::path from = boost::filesystem::path(location) / "non_existent_file.txt";
    boost::filesystem::path to = boost::filesystem::path(location) / "renamed_file.txt";
    
    ASSERT_THROW(safe_rename(from, to), std::runtime_error);
}

TEST_F(compaction_test, select_files_for_compaction) {
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


TEST_F(compaction_test, ensure_directory_exists_directory_exists) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "test_dir";
    boost::filesystem::create_directory(dir);
    
    ASSERT_NO_THROW(ensure_directory_exists(dir));
}

TEST_F(compaction_test, ensure_directory_exists_directory_created) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "test_dir";
    
    ASSERT_NO_THROW(ensure_directory_exists(dir));
    ASSERT_TRUE(boost::filesystem::exists(dir));
}

TEST_F(compaction_test, ensure_directory_exists_throws_exception) {
    boost::filesystem::path file = boost::filesystem::path(location) / "test_file.txt";
    
    boost::filesystem::ofstream ofs(file);
    ofs.close();
    
    ASSERT_THROW(ensure_directory_exists(file), std::runtime_error);
}

TEST_F(compaction_test, ensure_directory_exists_parent_directory_missing) {
    boost::filesystem::path dir = boost::filesystem::path(location) / "nonexistent_parent/test_dir";
    ASSERT_THROW(ensure_directory_exists(dir), std::runtime_error);
}


TEST_F(compaction_test, handle_existing_compacted_file_no_existing_files) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    
    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
}

TEST_F(compaction_test, handle_existing_compacted_file_with_existing_file) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::ofstream ofs(compacted_file);
    ofs.close();
    
    ASSERT_NO_THROW(handle_existing_compacted_file(location_path));
    ASSERT_TRUE(boost::filesystem::exists(location_path / "pwal_0000.compacted.prev"));
}

TEST_F(compaction_test, handle_existing_compacted_file_throws_exception) {
    boost::filesystem::path location_path = boost::filesystem::path(location);
    boost::filesystem::path compacted_file = location_path / "pwal_0000.compacted";
    boost::filesystem::path compacted_prev_file = location_path / "pwal_0000.compacted.prev";
    boost::filesystem::ofstream ofs1(compacted_file);
    ofs1.close();
    boost::filesystem::ofstream ofs2(compacted_prev_file);
    ofs2.close();
    
    ASSERT_THROW(handle_existing_compacted_file(location_path), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory) {
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

TEST_F(compaction_test, get_files_in_directory_directory_not_exists) {
    boost::filesystem::path non_existent_dir = boost::filesystem::path(location) / "non_existent_dir";
    ASSERT_THROW(get_files_in_directory(non_existent_dir), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory_not_a_directory) {
    boost::filesystem::path file_path = boost::filesystem::path(location) / "test_file.txt";
    boost::filesystem::ofstream ofs(file_path);
    ofs.close();

    ASSERT_THROW(get_files_in_directory(file_path), std::runtime_error);
}

TEST_F(compaction_test, get_files_in_directory_with_files) {
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

TEST_F(compaction_test, get_files_in_directory_empty_directory) {
    boost::filesystem::path empty_dir = boost::filesystem::path(location) / "empty_test_dir";
    boost::filesystem::create_directory(empty_dir);

    std::set<std::string> files = get_files_in_directory(empty_dir);
    EXPECT_TRUE(files.empty());
}


TEST_F(compaction_test, remove_file_safely_success) {
    boost::filesystem::path file = boost::filesystem::path(location) / "test_file_to_remove.txt";

    {
        boost::filesystem::ofstream ofs(file);
        ofs << "test content";
    }

    ASSERT_TRUE(boost::filesystem::exists(file));
    ASSERT_NO_THROW(remove_file_safely(file));
    ASSERT_FALSE(boost::filesystem::exists(file));
}

TEST_F(compaction_test, remove_file_safely_no_exception_for_nonexistent_file) {
    boost::filesystem::path file = boost::filesystem::path(location) / "non_existent_file.txt";
    ASSERT_NO_THROW(remove_file_safely(file));
}


// This test is disabled because it is environment-dependent and may not work properly in CI environments.
TEST_F(compaction_test, DISABLED_remove_file_safely_fails_to_remove_file) {
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


