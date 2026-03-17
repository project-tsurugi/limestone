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

 #pragma once

#include <limestone/api/datastore.h>
#include <limestone/logging.h>
#include <sys/stat.h>

#include <boost/filesystem.hpp>
#include <thread>

#include "compaction_catalog.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "manifest.h"
#include "online_compaction.h"
#include "test_root.h"

namespace limestone::testing {

using namespace limestone::api;

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern const std::string_view epoch_0_str;
extern const std::string_view epoch_0x100_str;
extern std::string data_manifest(int persistent_format_version = 1);
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;

class compaction_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/compaction_test";
    const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest::file_name);
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
        limestone::api::configuration conf{};
        conf.set_data_location(location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel();
        lc1_ = &datastore_->create_channel();
        lc2_ = &datastore_->create_channel();

        datastore_->ready();
        datastore_->wait_for_blob_file_garbace_collector();
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
    boost::filesystem::path path1001_;
    boost::filesystem::path path1002_;
    boost::filesystem::path path1003_;
    boost::filesystem::path path2001_;
    boost::filesystem::path path2002_;

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

        // Set up the on_rotate_log_files callback to signal when rotate_log_files() reaches the wait point
        test_datastore->on_rotate_log_files_callback = [&]() {
            std::unique_lock<std::mutex> lock(wait_mutex);
            wait_triggered = true;
            wait_cv.notify_one();  // Notify that on_rotate_log_files has been triggered
        };

        try {
            // Run compact_with_online in a separate thread
            auto future = std::async(std::launch::async, [&]() { datastore_->compact_with_online(); });

            // Wait for on_rotate_log_files to be triggered (simulating the waiting in rotate_log_files)
            {
                std::unique_lock<std::mutex> lock(wait_mutex);
                wait_cv.wait(lock, [&]() { return wait_triggered; });
            }

            // Now switch the epoch after on_rotate_log_files has been triggered
            datastore_->switch_epoch(epoch);

            // Wait for the compact operation to finish
            future.get();  // Will rethrow any exception from compact_with_online
            datastore_->wait_for_blob_file_garbace_collector();
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
                std::cout << "Entry Type: normal_entry, Storage ID: " << storage_id << ", Key: " << key << ", Value: " << value
                          << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                          << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
                break;
            }
            case log_entry::entry_type::normal_with_blob: {
                std::string value;
                entry.value(value);
                std::cout << "Entry Type: normal_with_blob, Storage ID: " << storage_id << ", Key: " << key << ", Value: " << value
                          << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                          << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) 
                          << ", Blob IDs: ";
                for (const auto& blob_id : entry.get_blob_ids()) {
                    std::cout << blob_id << " ";
                }
                std::cout << std::endl;
                break;
            }
            case log_entry::entry_type::remove_entry: {
                std::cout << "Entry Type: remove_entry, Storage ID: " << storage_id << ", Key: " << key
                          << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                          << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
                break;
            }
            case log_entry::entry_type::clear_storage:
            case log_entry::entry_type::add_storage:
            case log_entry::entry_type::remove_storage: {
                write_version_type write_version;
                entry.write_version(write_version);
                std::cout << "Entry Type: " << static_cast<int>(type) << ", Storage ID: " << storage_id
                          << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                          << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
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
            oss << files_expr << " which is " << ::testing::PrintToString(files) << ", contains " << match_count << " strings starting with " << prefix_expr
                << " which is \"" << prefix << "\", but expected " << expected_count_expr << " which is " << expected_count << ".";
            return ::testing::AssertionFailure() << oss.str();
        }
    }

    ::testing::AssertionResult ContainsString(const char* files_expr, const char* target_expr, const std::set<std::string>& files, const std::string& target) {
        if (files.find(target) != files.end()) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure() << files_expr << " (which is " << ::testing::PrintToString(files) << ") does not contain the string "
                                             << target_expr << " (which is \"" << target << "\").";
    }

    ::testing::AssertionResult ContainsCompactedFileInfo(const char* files_expr, const char* file_name_expr, const char* version_expr,
                                                         const std::set<compacted_file_info>& files, const std::string& file_name, int version) {
        compacted_file_info target(file_name, version);
        if (files.find(target) != files.end()) {
            return ::testing::AssertionSuccess();
        }

        std::ostringstream oss;
        oss << files_expr << " which is\n{\n";
        for (const auto& file : files) {
            oss << "   {file_name: \"" << file.get_file_name() << "\", version: " << file.get_version() << "},\n";
        }
        oss << "}\ndoes not contain the specified compacted_file_info {file_name: \"" << file_name << "\", version: " << version << "}.";

        return ::testing::AssertionFailure() << oss.str();
    }

    void create_manifest_file(int persistent_format_version = 1) {
        create_file(manifest_path, data_manifest(persistent_format_version));
        if (persistent_format_version > 1) {
            compaction_catalog catalog{location};
            catalog.update_catalog_file(0, 0, {}, {});
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
                                              const std::optional<epoch_id_type>& expected_epoch_number,
                                              const std::optional<std::uint64_t>& expected_minor_version, const std::vector<blob_id_type>& expected_blob_ids,
                                              log_entry::entry_type expected_type) {
        // Check the entry type
        if (entry.type() != expected_type) {
            return ::testing::AssertionFailure() << "Expected entry type: " << static_cast<int>(expected_type)
                                                 << ", but got: " << static_cast<int>(entry.type());
        }

        // Check the storage ID if it exists
        if (expected_storage_id.has_value()) {
            if (entry.storage() != expected_storage_id.value()) {
                return ::testing::AssertionFailure() << "Expected storage ID: " << expected_storage_id.value() << ", but got: " << entry.storage();
            }
        }

        // Check the key if it exists
        if (expected_key.has_value()) {
            std::string actual_key;
            entry.key(actual_key);
            if (actual_key != expected_key.value()) {
                return ::testing::AssertionFailure() << "Expected key: " << expected_key.value() << ", but got: " << actual_key;
            }
        }

        // Check the value if it exists
        if (expected_value.has_value()) {
            std::string actual_value;
            entry.value(actual_value);
            if (actual_value != expected_value.value()) {
                return ::testing::AssertionFailure() << "Expected value: " << expected_value.value() << ", but got: " << actual_value;
            }
        }

        // Check the write version if it exists
        if (expected_epoch_number.has_value() && expected_minor_version.has_value()) {
            epoch_id_type actual_epoch_number = log_entry::write_version_epoch_number(entry.value_etc());
            std::uint64_t actual_minor_version = log_entry::write_version_minor_write_version(entry.value_etc());

            if (actual_epoch_number != expected_epoch_number.value() || actual_minor_version != expected_minor_version.value()) {
                return ::testing::AssertionFailure() << "Expected write version (epoch_number: " << expected_epoch_number.value()
                                                     << ", minor_write_version: " << expected_minor_version.value()
                                                     << "), but got (epoch_number: " << actual_epoch_number << ", minor_write_version: " << actual_minor_version
                                                     << ")";
            }
        }

        // Check the blob IDs
        if (entry.type() == log_entry::entry_type::normal_with_blob) {
            std::vector<blob_id_type> actual_blob_ids = entry.get_blob_ids();
            if (actual_blob_ids.size() != expected_blob_ids.size()) {
                return ::testing::AssertionFailure() << "Expected blob IDs size: " << expected_blob_ids.size() << ", but got: " << actual_blob_ids.size();
            }

            for (std::size_t i = 0; i < expected_blob_ids.size(); ++i) {
                if (actual_blob_ids[i] != expected_blob_ids[i]) {
                    return ::testing::AssertionFailure() << "Expected blob ID: " << expected_blob_ids[i] << ", but got: " << actual_blob_ids[i];
                }
            }
        }
        // If all checks pass, return success
        return ::testing::AssertionSuccess();
    }

    // Generate a sorted file name list from a set of compacted_file_info
    static std::vector<std::string> get_sorted_list(const std::set<std::string>& set) {
        std::vector<std::string> list(set.begin(), set.end());
        std::sort(list.begin(), list.end());
        return list;
    }

    boost::filesystem::path create_dummy_blob_files(blob_id_type blob_id) {
        boost::filesystem::path path = datastore_->get_blob_file(blob_id).path();
        if (!boost::filesystem::exists(path)) {
            boost::filesystem::path dir = path.parent_path();
            if (!boost::filesystem::exists(dir)) {
                if (!boost::filesystem::create_directories(dir)) {
                    std::cerr << "Failed to create directory: " << dir.string() << std::endl;
                }
            }
            boost::filesystem::ofstream ofs(path, std::ios::binary);
            if (!ofs) {
                throw std::runtime_error("Failed to open file: " + path.string());
            }
            ofs << "dummy_blob_data";
        }
        return path;
    }

    void prepare_blob_gc_test_data() {
        // Epoch 3: Prepare initial entries.
        datastore_->switch_epoch(3);
    
        // Create two entries with blob data using lc0.
        lc0_->begin_session();
        lc0_->add_entry(1, "blob_key1", "blob_value1", {3, 0}, {1001, 1002});
        lc0_->add_entry(1, "blob_key2", "blob_value2", {3, 1}, {1003});
        lc0_->end_session();
    
        // Create two entries without blob data using lc0.
        lc0_->begin_session();
        lc0_->add_entry(1, "noblob_key1", "noblob_value1", {3, 2});
        lc0_->add_entry(1, "noblob_key2", "noblob_value2", {3, 3});
        lc0_->end_session();
    
        // Epoch 4: Switch epoch and update some entries with the same keys.
        datastore_->switch_epoch(4);
        lc0_->begin_session();
        // Update "blob_key1" with new blob data.
        lc0_->add_entry(1, "blob_key1", "blob_value1_epoch2", {4, 0}, {2001, 2002});
        // Update "noblob_key1" with a new value.
        lc0_->add_entry(1, "noblob_key1", "noblob_value1_epoch2", {4, 1});
        lc0_->end_session();
    
        // Create dummy blob files for the blob IDs.
        path1001_ = create_dummy_blob_files(1001);
        path1002_ = create_dummy_blob_files(1002);
        path1003_ = create_dummy_blob_files(1003);
        path2001_ = create_dummy_blob_files(2001);
        path2002_ = create_dummy_blob_files(2002);
        datastore_->set_next_blob_id(2003);

        // Set the available boundary version to 5.0
        datastore_->switch_available_boundary_version({5,0});
    }


    std::unique_ptr<backup_detail> begin_backup_with_epoch_switch(backup_type btype, epoch_id_type epoch) {
        std::mutex wait_mutex;
        std::condition_variable wait_cv;
        bool wait_triggered = false;
    
        auto* test_datastore = dynamic_cast<datastore_test*>(datastore_.get());
        if (test_datastore == nullptr) {
            throw std::runtime_error("datastore_ must be of type datastore_test");
        }
    
        test_datastore->on_rotate_log_files_callback = [&]() {
            std::unique_lock<std::mutex> lock(wait_mutex);
            wait_triggered = true;
            wait_cv.notify_one();
        };
    
        try {
            auto future = std::async(std::launch::async, [&]() { return datastore_->begin_backup(btype); });
    
            {
                std::unique_lock<std::mutex> lock(wait_mutex);
                wait_cv.wait(lock, [&]() { return wait_triggered; });
            }
    
            datastore_->switch_epoch(epoch);
            return future.get();
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            throw;
        }
    }
};

} // namespace limestone::testing
