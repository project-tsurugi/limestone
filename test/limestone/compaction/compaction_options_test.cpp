/*
 * Copyright 2022-2026 Project Tsurugi.
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
 #include "compaction_options.h"
 
 using namespace limestone::internal;
 namespace limestone::testing {
 
 class compaction_options_test : public ::testing::Test {
 protected:
     boost::filesystem::path from_dir_ = "/tmp/from";
     boost::filesystem::path to_dir_ = "/tmp/to";
     int num_workers_ = 4;
 };
 
 TEST_F(compaction_options_test, construct_without_file_set) {
     compaction_options options(from_dir_, to_dir_, num_workers_);
 
     EXPECT_EQ(options.get_from_dir(), from_dir_);
     EXPECT_EQ(options.get_to_dir(), to_dir_);
     EXPECT_EQ(options.get_num_worker(), num_workers_);
     EXPECT_FALSE(options.has_file_set());
     EXPECT_FALSE(options.is_gc_enabled());
 }
 
 TEST_F(compaction_options_test, construct_with_file_set_without_gc) {
     std::set<std::string> file_names = {"file1", "file2"};
 
     compaction_options options(from_dir_, to_dir_, num_workers_, file_names);
 
     EXPECT_EQ(options.get_from_dir(), from_dir_);
     EXPECT_EQ(options.get_to_dir(), to_dir_);
     EXPECT_EQ(options.get_num_worker(), num_workers_);
     EXPECT_TRUE(options.has_file_set());
     EXPECT_EQ(options.get_file_names(), file_names);
     EXPECT_FALSE(options.is_gc_enabled());
 }
 
 TEST_F(compaction_options_test, construct_with_file_set_and_gc) {
     std::set<std::string> file_names = {"file1", "file2"};
     write_version_type boundary_version(42, 5);
     auto gc_snapshot = std::make_unique<blob_file_gc_snapshot>(boundary_version);
     compaction_options options(from_dir_, to_dir_, num_workers_, file_names, std::move(gc_snapshot));
 
     EXPECT_EQ(options.get_from_dir(), from_dir_);
     EXPECT_EQ(options.get_to_dir(), to_dir_);
     EXPECT_EQ(options.get_num_worker(), num_workers_);
     EXPECT_TRUE(options.has_file_set());
     EXPECT_EQ(options.get_file_names(), file_names);
     EXPECT_TRUE(options.is_gc_enabled());
 }
 
 TEST_F(compaction_options_test, get_gc_snapshot_without_gc_enabled) {
     compaction_options options(from_dir_, to_dir_, num_workers_);
 
     EXPECT_FALSE(options.is_gc_enabled());
     EXPECT_THROW((void)options.get_gc_snapshot(), std::logic_error);
 }
 
 // New test for the constructor without to_dir (pre-compaction phase)
 TEST_F(compaction_options_test, construct_without_to_dir) {
     std::set<std::string> file_names = {"file1", "file2"};
 
     compaction_options options(from_dir_, num_workers_, file_names);
 
     EXPECT_EQ(options.get_from_dir(), from_dir_);
     EXPECT_EQ(options.get_to_dir(), boost::filesystem::path("/not_exists_dir"));
     EXPECT_EQ(options.get_num_worker(), num_workers_);
     EXPECT_TRUE(options.has_file_set());
     EXPECT_EQ(options.get_file_names(), file_names);
     EXPECT_FALSE(options.is_gc_enabled());
 }
 
 // The boundary epoch is unset by default (= the durable epoch serves as the
 // boundary) and can be set with the setter.
 TEST_F(compaction_options_test, boundary_epoch_is_unset_by_default_and_settable) {
     std::set<std::string> file_names = {"file1"};
     compaction_options options(from_dir_, to_dir_, num_workers_, file_names);
 
     EXPECT_FALSE(options.get_boundary_epoch().has_value());
 
     options.set_boundary_epoch(42);
     ASSERT_TRUE(options.get_boundary_epoch().has_value());
     EXPECT_EQ(options.get_boundary_epoch().value(), 42);
 }

 // The compacted WAL file of the current generation: if it is in the file set, it is taken out and recorded; nullopt
 // or a name not in the set does nothing
 TEST_F(compaction_options_test, compacted_input_file_name_is_taken_out_of_the_file_set) {
     std::set<std::string> file_names = {"pwal_0000.compacted.3", "pwal_0001"};
     compaction_options options(from_dir_, to_dir_, num_workers_, file_names);
     EXPECT_FALSE(options.get_compacted_input_file_name().has_value());

     options.set_compacted_input_file_name(std::nullopt);
     EXPECT_FALSE(options.get_compacted_input_file_name().has_value());
     EXPECT_EQ(options.get_file_names(), file_names);

     options.set_compacted_input_file_name("pwal_0000.compacted.2");  // not in the set
     EXPECT_FALSE(options.get_compacted_input_file_name().has_value());
     EXPECT_EQ(options.get_file_names(), file_names);

     options.set_compacted_input_file_name("pwal_0000.compacted.3");
     ASSERT_TRUE(options.get_compacted_input_file_name().has_value());
     EXPECT_EQ(options.get_compacted_input_file_name().value(), "pwal_0000.compacted.3");
     EXPECT_EQ(options.get_file_names(), std::set<std::string>{"pwal_0001"});
     EXPECT_TRUE(options.has_file_set());
 }

 // Taking the only file out of the set leaves the options with a file set (an empty set is scanned)
 TEST_F(compaction_options_test, compacted_input_file_name_leaves_an_empty_file_set) {
     compaction_options options(from_dir_, to_dir_, num_workers_, std::set<std::string>{"pwal_0000.compacted.3"});
     options.set_compacted_input_file_name("pwal_0000.compacted.3");
     EXPECT_TRUE(options.get_file_names().empty());
     EXPECT_TRUE(options.has_file_set());
 }
 
 }  // namespace limestone::testing
 
