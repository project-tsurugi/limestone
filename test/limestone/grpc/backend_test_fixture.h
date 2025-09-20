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

#pragma once

#include "limestone/compaction/compaction_test_fixture.h"
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <regex>
#include <boost/filesystem.hpp>
#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "limestone/api/configuration.h"
#include "limestone/api/datastore.h"
#include "limestone/blob/blob_test_helpers.h"
#include "limestone/grpc/service/message_versions.h"
#include "limestone/grpc/service/grpc_constants.h"
#include "test_root.h"
#include "wal_sync/wal_history.h"

namespace limestone::testing {

using limestone::grpc::proto::BackupObjectType;

class backend_test_fixture : public compaction_test_fixture {
protected:
    std::unique_ptr<blob_file_resolver> resolver_;
    epoch_id_type snapshot_epoch_id_;

    void prepare_backup_test_files() {
        datastore_->switch_epoch(1);
        lc0_->begin_session();
        create_blob_file(*resolver_, 200);
        lc0_->add_entry(1, "key1", "value1", {1,1}, {200});
        lc0_->end_session();
        datastore_->switch_epoch(2);
        lc0_->begin_session();
        lc0_->add_entry(1, "key1", "value1", {2,2});
        lc0_->end_session();
        run_compact_with_epoch_switch(3);
        datastore_->switch_epoch(3);
        snapshot_epoch_id_ = datastore_->get_impl()->get_compaction_catalog().get_max_epoch_id();
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {3,3});
        lc1_->end_session();
        datastore_->switch_epoch(4);
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {4,4});
        lc1_->end_session();
        datastore_->switch_epoch(5);
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {5,5});
        lc1_->end_session();
        datastore_->switch_epoch(6);
    }

    void prepare_backup_test_files_without_compaction() {
        datastore_->switch_epoch(1);
        lc0_->begin_session();
        create_blob_file(*resolver_, 200);
        lc0_->add_entry(1, "key1", "value1", {1,1}, {200});
        lc0_->end_session();
        datastore_->switch_epoch(2);
        lc0_->begin_session();
        lc0_->add_entry(1, "key1", "value1", {2,2});
        lc0_->end_session();
        datastore_->switch_epoch(3);
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {3,3});
        lc1_->end_session();
        datastore_->switch_epoch(4);
        datastore_->shutdown();
        datastore_ = nullptr;
        gen_datastore();
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {4,4});
        lc1_->end_session();
        datastore_->switch_epoch(5);
        lc1_->begin_session();
        lc1_->add_entry(1, "key1", "value1", {5,5});
        lc1_->end_session();
        datastore_->switch_epoch(6);
    }

    static std::string wildcard_to_regex(const std::string& pattern) {
        std::string regex;
        regex.reserve(pattern.size() * 2);
        for (char c : pattern) {
            switch (c) {
                case '*': regex += ".*"; break;
                case '.': regex += "\\."; break;
                default: regex += c; break;
            }
        }
        return regex;
    }

    template <typename Selector>
    void assert_backup_file_conditions(Selector selector) {
        namespace fs = boost::filesystem;
        fs::path dir(get_location());

        std::set<std::string> actual;
        for (auto& entry : fs::recursive_directory_iterator(dir)) {
            if (fs::is_regular_file(entry.status())) {
                fs::path rel = fs::relative(entry.path(), dir);
                std::string relstr = rel.generic_string();
                actual.insert(relstr);
            }
        }


        // Check for expected files
        for (const auto& cond : backup_conditions) {
            const std::string& pattern = selector(cond);
            if (pattern.empty()) {
                // If empty, the file must NOT exist
                bool found = false;
                for (const auto& act : actual) {
                    if (act.empty()) {
                        found = true;
                        break;
                    }
                }
                ASSERT_FALSE(found) << "Unexpected file found (should not exist): <empty pattern>";
                continue;
            }
            bool found = false;
            std::string regex_pat = wildcard_to_regex(pattern);
            std::regex re(regex_pat);
            for (const auto& act : actual) {
                if (std::regex_match(act, re)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << "Expected file not found: " << pattern << std::endl;
            }

            ASSERT_TRUE(found) << "Expected file pattern not found: " << pattern;
        }

        // Check for unexpected files
        for (const auto& act : actual) {
            bool matched = false;
            for (const auto& cond : backup_conditions) {
                const std::string& pattern = selector(cond);
                if (pattern.empty()) {
                    continue;
                }
                std::string regex_pat = wildcard_to_regex(pattern);
                std::regex re(regex_pat);
                if (std::regex_match(act, re)) {
                    matched = true;
                    break;
                }
            }
            ASSERT_TRUE(matched) << "Unexpected file found: " << act;
        }
    }

    void SetUp() override {
        compaction_test_fixture::SetUp();
        resolver_ = std::make_unique<blob_file_resolver>(boost::filesystem::path(get_location()));
    }
    void TearDown() override {
        resolver_.reset();
        compaction_test_fixture::TearDown();
    }

    // Structure representing backup file conditions for backup test
    struct backup_condition {
        std::string pre_rotation_path;
        std::string post_rotation_path;
        std::string object_id;
        std::string object_path;
        BackupObjectType object_type;
        bool is_online_backup_target;
        bool is_offline_backup_target;
    };

    const std::vector<backup_condition> backup_conditions =
        {{"blob/dir_00/00000000000000c8.blob", "blob/dir_00/00000000000000c8.blob", "", "", BackupObjectType::UNSPECIFIED, false, false},
         {"compaction_catalog", "compaction_catalog", "compaction_catalog", "compaction_catalog", BackupObjectType::METADATA, true,true},
         {"compaction_catalog.back", "compaction_catalog.back", "", "", BackupObjectType::UNSPECIFIED, false,   false},
         {"data/snapshot", "data/snapshot", "", "", BackupObjectType::UNSPECIFIED, false, false},
         {"epoch", "epoch", "epoch", "epoch", BackupObjectType::METADATA, false, true},
         {"", "epoch.*.6", "epoch.*.6", "epoch.*.6", BackupObjectType::METADATA, true, false},
         {"limestone-manifest.json", "limestone-manifest.json", "limestone-manifest.json", "limestone-manifest.json", BackupObjectType::METADATA, true, true},
         {"pwal_0000.*.0", "pwal_0000.*.0", "pwal_0000.*.0", "pwal_0000.*.0", BackupObjectType::LOG, true, true},
         {"pwal_0000.compacted", "pwal_0000.compacted", "pwal_0000.compacted", "pwal_0000.compacted", BackupObjectType::SNAPSHOT, true, true},
         {"pwal_0001", "pwal_0001.*.0", "pwal_0001.*.0", "pwal_0001.*.0", BackupObjectType::LOG, true, false},
         {"pwal_0001", "pwal_0001.*.0", "pwal_0001", "pwal_0001", BackupObjectType::LOG, false, true},
         {"wal_history", "wal_history", "wal_history", "wal_history", BackupObjectType::METADATA, true, true}};


    // get filtered backup_condition list
    template <typename Filter>
    std::vector<backup_condition> get_filtered_backup_conditions(Filter filter) {
        std::vector<backup_condition> result;
        for (const auto& cond : backup_conditions) {
            if (filter(cond)) {
                result.push_back(cond);
            }
        }
        return result;
    }

    // Find matching backup conditions based on object_id with wildcard support
    std::vector<backup_condition> find_matching_backup_conditions(
        const std::string& object_id,
        const std::vector<backup_condition>& conditions) {
        std::vector<backup_condition> result;

        for (const auto& cond : conditions) {
            std::string regex_pattern = wildcard_to_regex(cond.object_id);
            std::regex re(regex_pattern);
            if (std::regex_match(object_id, re)) {
                result.push_back(cond);
            }
        }

        return result;
    }

    // Check if the actual path matches the expected path with wildcard support
    bool is_path_matching(const std::string& actual_path, const std::string& expected_path) {
        std::string regex_pattern = wildcard_to_regex(expected_path);
        std::regex re(regex_pattern);
        return std::regex_match(actual_path, re);
    }
};

}  // namespace limestone::testing
