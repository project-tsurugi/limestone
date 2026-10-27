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
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "dblog_scan.h"
#include "limestone/log/log_entry_bytes.h"
#include "log_entry/log_entry_format.h"
#include "sort/sorting_directory.h"
#include "sort/wal_sorter.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::internal::dblog_scan;
using limestone::internal::log_entry_view;
using limestone::internal::range_deletions;
using limestone::internal::sort_settings;
using limestone::internal::sorting_directory;
using limestone::internal::wal_sorter;

namespace format = limestone::internal::log_entry_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;

class wal_sorter_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/wal_sorter_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    /**
     * @brief Writes a pwal file.
     * @param name the file name
     * @param bytes the contents
     */
    static void write_pwal(const std::string& name, const std::string& bytes) {
        std::ofstream out((boost::filesystem::path(location) / name).string(), std::ios::binary);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good()) << "cannot write " << name;
    }
    /**
     * @brief Runs merge() and collects the bytes of the output entries.
     */
    static std::vector<std::string> merge_output(wal_sorter& sorter) {
        std::vector<std::string> output;
        sorter.merge(std::nullopt, [&output](const log_entry_view& view) {
            output.emplace_back(view.serialized_entry());
        });
        return output;
    }
    /**
     * @brief Returns the path of the sorting directory.
     */
    static boost::filesystem::path sorting_path() {
        return boost::filesystem::path(location) / sorting_directory::name;
    }
};

// scanning and merging 2 WAL files gives the result in key_sid order, with duplicates removed and the range
// deletions applied. The result of the scan has the greatest epoch, the greatest BLOB ID and the range deletions
// (clear_storage and remove_storage), and the callback of normal_with_blob is called. add_storage is ignored
TEST_F(wal_sorter_test, scan_and_merge) {
    constexpr auto clear_storage = static_cast<std::uint8_t>(format::entry_type::clear_storage);
    constexpr auto add_storage = static_cast<std::uint8_t>(format::entry_type::add_storage);
    constexpr auto remove_storage = static_cast<std::uint8_t>(format::entry_type::remove_storage);
    const std::string a1 = normal_entry(1, "a", "a1", 10, 0);
    const std::string b1 = normal_entry(1, "b", "b1", 10, 1);
    const std::string x1 = normal_entry(2, "x", "x1", 10, 2);
    const std::string y1 = normal_with_blob(3, "y", "y1", 10, 3, {7});
    const std::string add4 = storage_entry(add_storage, 4, 10, 4);
    const std::string a2 = normal_entry(1, "a", "a2", 11, 0);
    const std::string b2 = remove_entry(1, "b", 11, 1);
    const std::string c1 = normal_with_blob(1, "c", "c1", 11, 2, {5, 9});
    const std::string clear2 = storage_entry(clear_storage, 2, 11, 3);
    const std::string remove3 = storage_entry(remove_storage, 3, 11, 4);
    const std::string z1 = normal_entry(3, "z", "z1", 11, 5);
    write_pwal("pwal_0000", marker(2, 10) + a1 + b1 + x1 + y1 + add4);
    write_pwal("pwal_0001", marker(2, 11) + a2 + b2 + c1 + clear2 + remove3 + z1);

    dblog_scan scan{boost::filesystem::path(location)};
    const sort_settings settings{2, 1024 * 1024, 128};
    wal_sorter sorter(scan, settings, location);
    std::vector<std::string> with_blob;
    const auto result = sorter.scan(std::nullopt, 11, [&with_blob](const log_entry& entry) {
        with_blob.emplace_back(entry.view().serialized_entry());
    });
    EXPECT_EQ(result.max_appeared_epoch, 11U);
    EXPECT_EQ(result.max_blob_id, 9U);
    EXPECT_EQ(result.deletions, (range_deletions{{2, write_version_type{11, 3}}, {3, write_version_type{11, 4}}}));
    std::sort(with_blob.begin(), with_blob.end());  // the order of arrival from the 2 threads is not fixed
    EXPECT_EQ(with_blob, (std::vector<std::string>{c1, y1}));
    EXPECT_TRUE(boost::filesystem::exists(sorting_path()));

    // x1 is older than the clear_storage (11, 3) of storage 2, and y1 is older than the remove_storage (11, 4) of
    // storage 3, so they are excluded. z1 is newer than the remove_storage, so it remains
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{a2, b2, c1, z1}));
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// without a WAL file, the result of the scan is empty and the merge outputs nothing
TEST_F(wal_sorter_test, no_input) {
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{}, location);
    const auto result = sorter.scan(std::nullopt, 5, {});
    EXPECT_EQ(result.max_appeared_epoch, 5U);
    EXPECT_EQ(result.max_blob_id, 0U);
    EXPECT_TRUE(result.deletions.empty());
    EXPECT_TRUE(merge_output(sorter).empty());
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// a failure of the scan (a broken file) is thrown, and sorting is removed by the destructor
TEST_F(wal_sorter_test, scan_failure_is_thrown_and_sorting_is_removed) {
    write_pwal("pwal_0000", marker(2, 10) + normal_entry(1, "a", "a1", 10, 0).substr(0, 5));  // truncated
    dblog_scan scan{boost::filesystem::path(location)};
    {
        wal_sorter sorter(scan, sort_settings{}, location);
        EXPECT_THROW((void) sorter.scan(std::nullopt, 10, {}), limestone_exception);
        EXPECT_TRUE(boost::filesystem::exists(sorting_path()));
    }
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// merge() before scan(), a second scan() and a second merge() are FATAL
TEST_F(wal_sorter_test, call_order_violations_are_fatal) {
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{}, location);
    EXPECT_DEATH(sorter.merge(std::nullopt, [](const log_entry_view&) {}),
                 "wal_sorter:merge merge\\(\\) is called before scan\\(\\)");
    (void) sorter.scan(std::nullopt, 1, {});
    EXPECT_DEATH((void) sorter.scan(std::nullopt, 1, {}), "wal_sorter:scan scan\\(\\) is called twice");
    (void) merge_output(sorter);
    EXPECT_DEATH(sorter.merge(std::nullopt, [](const log_entry_view&) {}),
                 "wal_sorter:merge merge\\(\\) is called twice");
}

}  // namespace limestone::testing
