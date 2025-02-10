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

#include <gtest/gtest.h>
#include <iostream>  
#include <boost/filesystem.hpp>
#include "compaction_catalog.h"
#include "limestone/api/epoch_id_type.h"
#include "limestone/api/limestone_exception.h"
#include "internal.h"

namespace limestone::testing {

using limestone::internal::compacted_file_info;
using limestone::internal::compaction_catalog;
using limestone::api::limestone_io_exception;

class mock_file_operations : public limestone::internal::real_file_operations {
public:
    void directory_iterator_next(boost::filesystem::directory_iterator& it, boost::system::error_code& ec) {
        ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
    };
};

class assemble_snapshot_input_filenames_test : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up temporary log directory for the test
        if (system("rm -rf /tmp/assemble_snapshot_input_filenames_test") != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/assemble_snapshot_input_filenames_test") != 0) {
            std::cerr << "Cannot create directory" << std::endl;
        }

        // Initialize the compaction catalog with a valid log directory path
        log_location_ = "/tmp/assemble_snapshot_input_filenames_test";
        compaction_catalog_ = std::make_unique<compaction_catalog>(log_location_);
    }

    void TearDown() override {
        // Clean up temporary log directory after the test
        if (system("rm -rf /tmp/assemble_snapshot_input_filenames_test") != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
    }

    void clear_compaction_catalog() {
        compacted_files_.clear();
        detached_pwals_.clear();
    }

    void add_detached_pwals(const std::initializer_list<std::string>& pwals) {
        detached_pwals_.insert(pwals.begin(), pwals.end());
        compaction_catalog_->update_catalog_file(0, 0, compacted_files_, detached_pwals_);
    }

    std::unique_ptr<compaction_catalog> compaction_catalog_;
    boost::filesystem::path log_location_;

    std::set<compacted_file_info> compacted_files_;
    std::set<std::string> detached_pwals_;
};

    TEST_F(assemble_snapshot_input_filenames_test, retrieves_filenames_correctly) {
        // Prepare some files in the log location directory
        std::ofstream(log_location_ / "pwal_0001");
        std::ofstream(log_location_ / "pwal_0002");
        std::ofstream(log_location_ / "pwal_0003");
        std::ofstream(log_location_ / "pwal_0004");

        // Simulate detached PWALs in the compaction catalog
        add_detached_pwals({"pwal_0001", "pwal_0002"});

        // Get the filenames that should be used for the snapshot
        std::set<std::string> filenames;
        filenames = assemble_snapshot_input_filenames(compaction_catalog_, log_location_);

        // Ensure the correct files are retrieved
        EXPECT_EQ(filenames.size(), 2);
        EXPECT_NE(filenames.find("pwal_0003"), filenames.end());
        EXPECT_NE(filenames.find("pwal_0004"), filenames.end());

        // Ensure the detached PWALs are not included
        std::ofstream(log_location_ / compaction_catalog::get_compacted_filename());
        filenames = assemble_snapshot_input_filenames(compaction_catalog_, log_location_);
        EXPECT_EQ(filenames.size(), 2);
        EXPECT_NE(filenames.find("pwal_0003"), filenames.end());
        EXPECT_NE(filenames.find("pwal_0004"), filenames.end());
    }

    TEST_F(assemble_snapshot_input_filenames_test, throws_exception_when_directory_does_not_exist) {
        // Set a non-existent directory
        boost::filesystem::path non_existent_directory = "/tmp/non_existent_directory";

        // Check if an exception is thrown
        try {
            auto filenames = assemble_snapshot_input_filenames(compaction_catalog_, non_existent_directory);
            FAIL() << "Expected an exception to be thrown";
        } catch (const limestone_io_exception& e) {
            // Check if the exception message contains the expected content
            std::string expected_message = "Failed to initialize directory iterator, path:";
            std::string actual_message = e.what();
            std::cout << "Caught expected exception: " << actual_message << std::endl;
            EXPECT_TRUE(actual_message.find(expected_message) != std::string::npos) << "Expected exception message to contain: " << expected_message;
        } catch (...) {
            FAIL() << "Expected a limestone_io_exception, but got a different exception";
        }
    }

    TEST_F(assemble_snapshot_input_filenames_test, throws_exception_when_directory_iterator_increment) {
        // Prepare some files in the log location directory
        std::ofstream(log_location_ / "pwal_0001");
        std::ofstream(log_location_ / "pwal_0002");
        std::ofstream(log_location_ / "pwal_0003");
        std::ofstream(log_location_ / "pwal_0004");

        // Check if an exception is thrown
        try {
            mock_file_operations file_ops;
            auto filenames = assemble_snapshot_input_filenames(compaction_catalog_,log_location_, file_ops);
            FAIL() << "Expected an exception to be thrown";
        } catch (const limestone_io_exception& e) {
            // Check if the exception message contains the expected content
            std::string expected_message = "Failed to access directory entry, path:";
            std::string actual_message = e.what();
            std::cout << "Caught expected exception: " << actual_message << std::endl;
            EXPECT_TRUE(actual_message.find(expected_message) != std::string::npos) << "Expected exception message to contain: " << expected_message;
        } catch (...) {
            FAIL() << "Expected a limestone_io_exception, but got a different exception";
        }
    }

    TEST_F(assemble_snapshot_input_filenames_test, handles_empty_directory) {
        // No files are created in the directory
        auto filenames = assemble_snapshot_input_filenames(compaction_catalog_, log_location_);

        // Ensure that no files are retrieved
        EXPECT_TRUE(filenames.empty());
    }

}  // namespace limestone::testing
