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

// Forward declare the target function for testing
namespace limestone::internal {
    std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location);
std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location,
    file_operations& file_ops);
}


namespace limestone::testing {

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern const std::string_view epoch_0_str;
extern const std::string_view epoch_0x100_str;
extern std::string data_manifest(int persistent_format_version = 1);
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;

class epoch_file_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/epoch_file_test";
    const boost::filesystem::path manifest_path = boost::filesystem::path(location) / std::string(limestone::internal::manifest_file_name);
    const boost::filesystem::path compaction_catalog_path = boost::filesystem::path(location) / "compaction_catalog";
    const boost::filesystem::path epoch_file_path = boost::filesystem::path(location) / std::string(limestone::internal::epoch_file_name);
    const boost::filesystem::path pwal000_file_path = boost::filesystem::path(location) / "pwal_0000";
    const std::string compacted_filename = compaction_catalog::get_compacted_filename();

    void SetUp() {
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
        compaction_catalog_ = std::make_unique<compaction_catalog>(boost::filesystem::path(location));
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel(location);
        lc1_ = &datastore_->create_channel(location);

        datastore_->ready();
    }

    epoch_id_type last_durable_epoch() {
        boost::filesystem::path from_dir = boost::filesystem::path(location);
        std::set<std::string> file_names = assemble_snapshot_input_filenames(compaction_catalog_, from_dir);
        dblog_scan logscan = file_names.empty() ? dblog_scan{from_dir} : dblog_scan{from_dir, file_names};
        epoch_id_type last_durable_epoch = logscan.last_durable_epoch_in_dir();
        return last_durable_epoch;
    }

    std::optional<boost::filesystem::path> get_rotated_epoch_file() {
        std::optional<boost::filesystem::path> result;
        boost::filesystem::path directory = boost::filesystem::path(location);
        if (boost::filesystem::exists(directory) && boost::filesystem::is_directory(directory)) {
            for (const auto& entry : boost::filesystem::directory_iterator(directory)) {
                if (boost::filesystem::is_regular_file(entry) && entry.path().filename().string().rfind("epoch.", 0) == 0) {
                    if (result.has_value()) {
                        throw std::runtime_error("Multiple files starting with 'epoch.' found.");
                    }
                    result = entry.path();
                }
            }
        }
        return result;
    }

    void TearDown() {
        datastore_ = nullptr;
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    std::unique_ptr<compaction_catalog> compaction_catalog_;
    log_channel* lc0_{};
    log_channel* lc1_{};
};


TEST_F(epoch_file_test, last_durable_epoch) {
    // ログディレクトリを初期化
    gen_datastore();
    datastore_->shutdown();
    datastore_ = nullptr;

    // Empty epoch file, No rotated epoch files
    ASSERT_EQ(0, boost::filesystem::file_size(epoch_file_path));
    ASSERT_FALSE(get_rotated_epoch_file().has_value());
    EXPECT_EQ(0, last_durable_epoch());

    // No epoch file, No rotated epoch files
    boost::filesystem::remove(epoch_file_path);
    ASSERT_FALSE(boost::filesystem::exists(epoch_file_path));
    ASSERT_FALSE(get_rotated_epoch_file().has_value());
    EXPECT_EQ(0, last_durable_epoch());

    // Non-empty epoch file, No rotated epoch files
    gen_datastore();
    datastore_->switch_epoch(1);
    datastore_->switch_epoch(2);
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v1", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(3);
    ASSERT_TRUE(boost::filesystem::file_size(epoch_file_path) > 0);
    ASSERT_FALSE(get_rotated_epoch_file().has_value());
    EXPECT_EQ(2, last_durable_epoch());


    // Empty epoch file, Non-empty rotated epoch files
    datastore_->rotate_epoch_file();

    ASSERT_EQ(0, boost::filesystem::file_size(epoch_file_path));
    ASSERT_TRUE(get_rotated_epoch_file().has_value());
    ASSERT_TRUE(boost::filesystem::file_size(get_rotated_epoch_file().value()) > 0);
    EXPECT_EQ(2, last_durable_epoch());

    // No epoch file, Non-empty rotated epoch files
    boost::filesystem::remove(epoch_file_path);
    ASSERT_FALSE(boost::filesystem::exists(epoch_file_path));
    ASSERT_TRUE(get_rotated_epoch_file().has_value());
    ASSERT_TRUE(boost::filesystem::file_size(get_rotated_epoch_file().value()) > 0);
    EXPECT_EQ(2, last_durable_epoch());

    // Non-empty epoch file, Non-empty rotated epoch files
    lc0_->begin_session();
    lc0_->add_entry(1, "k1", "v2", {1, 0});
    lc0_->end_session();
    datastore_->switch_epoch(4);

    ASSERT_TRUE(boost::filesystem::file_size(epoch_file_path) > 0);
    ASSERT_TRUE(boost::filesystem::file_size(get_rotated_epoch_file().value()) > 0);
    EXPECT_EQ(3, last_durable_epoch());
}



} // namespace limestone::testing

