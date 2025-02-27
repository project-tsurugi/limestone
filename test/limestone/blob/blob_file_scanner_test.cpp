/*
 * Copyright 2022-2025 Project Tsurugi.
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
 #include <boost/filesystem.hpp>
 #include <fstream>
 #include <set>
 #include "blob_file_scanner.h"
 #include "blob_file_resolver.h"
 
 namespace limestone::testing {
 
 class blob_file_scanner_test : public ::testing::Test {
 protected:
     // Base directory for test files
     const std::string base_directory = "/tmp/blob_file_scanner_test";
     // Unique pointers to resolver and scanner
     std::unique_ptr<limestone::internal::blob_file_resolver> resolver_;
     std::unique_ptr<limestone::internal::blob_file_scanner> scanner_;
 
     // Setup function to initialize test environment
     void SetUp() override {
         // Remove any existing test directory and create a new one
         boost::filesystem::remove_all(base_directory);
         boost::filesystem::create_directories(base_directory);
 
         // Initialize resolver with the base directory and a threshold of 10
         resolver_ = std::make_unique<limestone::internal::blob_file_resolver>(
             boost::filesystem::path(base_directory));
         // Initialize scanner with the resolver
         scanner_ = std::make_unique<limestone::internal::blob_file_scanner>(*resolver_);
 
         // Create 100 subdirectories named dir_00 to dir_99
         for (std::size_t i = 0; i < 100; ++i) {
             std::ostringstream dir_name;
             dir_name << "dir_" << std::setw(2) << std::setfill('0') << i;
             boost::filesystem::create_directories(resolver_->get_blob_root() / dir_name.str());
         }
     }
 
     // Teardown function to clean up after tests
     void TearDown() override {
         // Remove the test directory and all its contents
         boost::filesystem::remove_all(base_directory);
     }
 
     // Helper function to create a dummy blob file with the given ID
     void create_blob_file(uint64_t id) {
         // Resolve the path for the given blob ID
         auto path = resolver_->resolve_path(id);
         // Create and write dummy data to the file
         std::ofstream ofs(path.string());
         ofs << "dummy data";
     }
 };
 
 // Test case to verify that the scanner finds all blob files
 TEST_F(blob_file_scanner_test, scan_finds_all_blob_files) {
     // Create dummy blob files with IDs 100, 200, and 300
     create_blob_file(100);
     create_blob_file(200);
     create_blob_file(300);
 
     // Set to store found file paths
     std::set<boost::filesystem::path> found_files;
 
     // Iterate over the scanner to collect found blob file paths
     for (const auto& path : *scanner_) {
         found_files.insert(path);
     }
 
     // Verify that exactly 3 files were found
     EXPECT_EQ(found_files.size(), 3);
     // Check that each created blob file is in the set of found files
     EXPECT_TRUE(found_files.count(resolver_->resolve_path(100)) > 0);
     EXPECT_TRUE(found_files.count(resolver_->resolve_path(200)) > 0);
     EXPECT_TRUE(found_files.count(resolver_->resolve_path(300)) > 0);
 }
 
 // Test case to verify that the scanner ignores non-blob files
 TEST_F(blob_file_scanner_test, scan_ignores_non_blob_files) {
     // Create a dummy blob file with ID 100
     create_blob_file(100);
     // Create a non-blob file named "non_blob.txt" in "dir_00"
     std::ofstream non_blob_file((resolver_->get_blob_root() / "dir_00" / "non_blob.txt").string());
     non_blob_file << "not a blob";
 
     // Set to store found file paths
     std::set<boost::filesystem::path> found_files;
 
     // Iterate over the scanner to collect found blob file paths
     for (const auto& path : *scanner_) {
         found_files.insert(path);
     }
 
     // Verify that only 1 file was found
     EXPECT_EQ(found_files.size(), 1);
     // Check that the created blob file is in the set of found files
     EXPECT_TRUE(found_files.count(resolver_->resolve_path(100)) > 0);
 }
 
 // Test case to verify that the scanner handles an empty directory correctly
 TEST_F(blob_file_scanner_test, scan_handles_empty_directory) {
     // Set to store found file paths
     std::set<boost::filesystem::path> found_files;
 
     // Iterate over the scanner in an empty directory
     for (const auto& path : *scanner_) {
         found_files.insert(path);
     }
 
     // Verify that no files were found
     EXPECT_TRUE(found_files.empty());
 }
 
 }  // namespace limestone::testing
 