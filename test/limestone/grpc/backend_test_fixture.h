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



    void SetUp() override {
        compaction_test_fixture::SetUp();
        resolver_ = std::make_unique<blob_file_resolver>(boost::filesystem::path(get_location()));
    }
    void TearDown() override {
        resolver_.reset();
        compaction_test_fixture::TearDown();
    }
};

} // namespace limestone::testing
