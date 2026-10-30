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
#include <mutex>
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
using limestone::api::limestone_io_exception;
using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::internal::beyond_boundary_handling;
using limestone::internal::dblog_scan;
using limestone::internal::log_entry_view;
using limestone::internal::range_deletions;
using limestone::internal::sort_settings;
using limestone::internal::sorted_file_merger;
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
     * @brief Returns the path of the sorting directory (the entry of location whose name begins with the prefix), or
     *        an empty path if there is none.
     */
    static boost::filesystem::path sorting_path() {
        for (const auto& e : boost::filesystem::directory_iterator(location)) {
            if (e.path().filename().string().rfind(sorting_directory::name_prefix, 0) == 0) {
                return e.path();
            }
        }
        return {};
    }
    /**
     * @brief Writes the WAL files containing normal_with_blob in invalid snippets (one of epoch 12 beyond the
     *        boundary 11, and an already invalidated one). The BLOB ID of the valid snippet is 3, and those of the
     *        invalid snippets are 9 and 20
     * @return the valid entry (the output of the merge)
     */
    static std::string write_pwals_with_invalid_snippets() {
        constexpr auto marker_begin = static_cast<std::uint8_t>(format::entry_type::marker_begin);
        constexpr auto marker_invalidated_begin =
                static_cast<std::uint8_t>(format::entry_type::marker_invalidated_begin);
        const std::string a1 = normal_with_blob(1, "a", "a1", 10, 0, {3});
        const std::string b1 = normal_with_blob(1, "b", "b1", 12, 0, {9});
        const std::string c1 = normal_with_blob(1, "c", "c1", 9, 0, {20});
        write_pwal("pwal_0000", marker(marker_begin, 10) + a1 + marker(marker_begin, 12) + b1);
        write_pwal("pwal_0001", marker(marker_invalidated_begin, 9) + c1);
        return a1;
    }
    /**
     * @brief Counts the files in the sorting directory.
     */
    static std::size_t files_in_sorting() {
        std::size_t n = 0;
        for (const auto& e : boost::filesystem::directory_iterator(sorting_path())) {
            (void) e;
            ++n;
        }
        return n;
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
    std::mutex mutex;
    std::vector<std::string> with_blob;
    const auto result =
        sorter.scan(11, beyond_boundary_handling::invalidate, [&mutex, &with_blob](const log_entry& entry) {
            const std::lock_guard<std::mutex> lock(mutex);  // called concurrently from the 2 threads
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

// two range deletions of a storage keep the newer write_version, whether the older one comes first or later.
// One thread processes the files in order: the older one comes first for storage 2, and later for storage 3
TEST_F(wal_sorter_test, deletions_of_a_storage_keep_the_newest_version) {
    constexpr auto clear_storage = static_cast<std::uint8_t>(format::entry_type::clear_storage);
    const std::string x1 = normal_entry(2, "x", "x1", 10, 0);
    const std::string x2 = normal_entry(2, "x", "x2", 12, 0);
    const std::string y1 = normal_entry(3, "y", "y1", 10, 1);
    const std::string y2 = normal_entry(3, "y", "y2", 12, 1);
    write_pwal("pwal_0000", marker(2, 10) + x1 + y1 + storage_entry(clear_storage, 2, 10, 2)
                                + storage_entry(clear_storage, 3, 11, 2));
    write_pwal("pwal_0001", marker(2, 12) + x2 + y2 + storage_entry(clear_storage, 2, 11, 3)
                                + storage_entry(clear_storage, 3, 10, 3));
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, 1024 * 1024, 128}, location);
    const auto result = sorter.scan(12, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(result.deletions, (range_deletions{{2, write_version_type{11, 3}}, {3, write_version_type{11, 2}}}));
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{x2, y2}));
}

// both with invalidate and with skip (the online compaction), the BLOB IDs of the invalid snippets are included in
// the maximum. Their entries are passed neither to the sort nor to the BLOB GC
TEST_F(wal_sorter_test, max_blob_id_includes_the_blob_ids_of_invalid_snippets) {
    for (const auto handling : {beyond_boundary_handling::invalidate, beyond_boundary_handling::skip}) {
        const std::string a1 = write_pwals_with_invalid_snippets();  // written each time: invalidate rewrites markers
        dblog_scan scan{boost::filesystem::path(location)};
        wal_sorter sorter(scan, sort_settings{2, 1024 * 1024, 128}, location);
        std::vector<std::string> with_blob;
        std::mutex mutex;
        const auto result = sorter.scan(11, handling, [&mutex, &with_blob](const log_entry& entry) {
            const std::lock_guard<std::mutex> lock(mutex);
            with_blob.emplace_back(entry.view().serialized_entry());
        });
        EXPECT_EQ(result.max_blob_id, 20U);
        EXPECT_EQ(with_blob, (std::vector<std::string>{a1}));
        EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{a1}));
    }
}

// without a WAL file, the result of the scan is empty and the merge outputs nothing
TEST_F(wal_sorter_test, no_input) {
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{}, location);
    const auto result = sorter.scan(5, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(result.max_appeared_epoch, 5U);
    EXPECT_EQ(result.max_blob_id, 0U);
    EXPECT_TRUE(result.deletions.empty());
    EXPECT_TRUE(merge_output(sorter).empty());
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// a failure of the scan (a broken file) is thrown, merge() is FATAL after it, and sorting is removed by the
// destructor
TEST_F(wal_sorter_test, scan_failure_is_thrown_and_sorting_is_removed) {
    write_pwal("pwal_0000", marker(2, 10) + normal_entry(1, "a", "a1", 10, 0).substr(0, 5));  // truncated
    dblog_scan scan{boost::filesystem::path(location)};
    {
        wal_sorter sorter(scan, sort_settings{}, location);
        EXPECT_THROW((void) sorter.scan(10, beyond_boundary_handling::invalidate, {}), limestone_exception);
        EXPECT_TRUE(boost::filesystem::exists(sorting_path()));
        EXPECT_DEATH(sorter.merge(std::nullopt, [](const log_entry_view&) {}),
                     R"(wal_sorter:merge merge\(\) is called without a successful scan\(\))");
    }
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// a failure of writing a sorted file (sorting is removed in the callback, so finish() cannot write) is thrown by
// scan() as limestone_io_exception
TEST_F(wal_sorter_test, write_failure_of_a_sorted_file_is_thrown_by_scan) {
    write_pwal("pwal_0000", marker(2, 10) + normal_with_blob(1, "a", "a1", 10, 0, {1}));
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, 1024 * 1024, 128}, location);
    EXPECT_THROW((void) sorter.scan(10, beyond_boundary_handling::invalidate, [](const log_entry&) {
        boost::filesystem::remove_all(sorting_path());
    }), limestone_io_exception);
}

// merge() before scan(), a second scan() and a second merge() are FATAL
TEST_F(wal_sorter_test, call_order_violations_are_fatal) {
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{}, location);
    EXPECT_DEATH(sorter.merge(std::nullopt, [](const log_entry_view&) {}),
                 R"(wal_sorter:merge merge\(\) is called without a successful scan\(\))");
    (void) sorter.scan(1, beyond_boundary_handling::invalidate, {});
    EXPECT_DEATH((void) sorter.scan(1, beyond_boundary_handling::invalidate, {}),
                 "wal_sorter:scan scan\\(\\) is called twice");
    (void) merge_output(sorter);
    EXPECT_DEATH(sorter.merge(std::nullopt, [](const log_entry_view&) {}),
                 "wal_sorter:merge merge\\(\\) is called twice");
}

// a multistage merge: with fan-in 2, five sorted files (the arena holds two entries) go through intermediate merges
// and only the latest version of each key remains, in the order of key_sid
TEST_F(wal_sorter_test, multistage_merge) {
    std::vector<std::string> v1;
    std::vector<std::string> v2;
    for (int i = 0; i < 5; ++i) {
        v1.push_back(normal_entry(1, "k" + std::to_string(i), "a", 1, 0));
        v2.push_back(normal_entry(1, "k" + std::to_string(i), "b", 2, 0));
    }
    write_pwal("pwal_0000",
               marker(2, 2) + v1[3] + v1[1] + v2[3] + v1[0] + v1[2] + v2[1] + v1[4] + v2[0] + v2[2] + v2[4]);
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, v1[0].size() * 2, 2}, location);
    (void) sorter.scan(2, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(files_in_sorting(), 5U);
    EXPECT_EQ(merge_output(sorter), v2);
    EXPECT_FALSE(boost::filesystem::exists(sorting_path()));
}

// one sorted file: its contents are output as they are, without an intermediate merge
TEST_F(wal_sorter_test, one_sorted_file) {
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string b = normal_entry(1, "b", "v", 1, 1);
    write_pwal("pwal_0000", marker(2, 1) + b + a);
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, 1024 * 1024, 128}, location);
    (void) sorter.scan(1, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(files_in_sorting(), 1U);
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{a, b}));
}

// an entry larger than the arena becomes a sorted file of its own, and is output at its position in the order
TEST_F(wal_sorter_test, oversized_entry) {
    const std::string s0 = normal_entry(1, "k0", "v", 1, 0);
    const std::string s1 = normal_entry(1, "k1", "v", 1, 1);
    const std::string s2 = normal_entry(1, "k2", "v", 1, 2);
    const std::size_t arena = s0.size() * 2;
    const std::string big = normal_entry(1, "k5", std::string(arena, 'x'), 1, 3);
    write_pwal("pwal_0000", marker(2, 1) + s0 + big + s1 + s2);
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, arena, 128}, location);
    (void) sorter.scan(1, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(files_in_sorting(), 3U);  // the file of big alone, (s0, s1), (s2)
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{s0, s1, s2, big}));
}

// a compacted WAL file given to the final merge joins the inputs with its leading marker_begin skipped; for the same
// key the newer version of the WAL wins, and the callback is called for the normal_with_blob entries in the file
TEST_F(wal_sorter_test, compacted_wal_file_in_the_final_merge) {
    const std::string a1 = normal_entry(1, "a", "old", 0, 0);
    const std::string c0 = normal_with_blob(1, "c", "c0", 0, 0, {3});
    const std::string e0 = remove_entry(1, "e", 0, 0);
    const std::string a2 = normal_entry(1, "a", "new", 10, 0);
    const std::string d1 = normal_entry(1, "d", "d1", 10, 1);
    write_pwal("pwal_0000", marker(2, 10) + a2 + d1);
    const boost::filesystem::path compacted_dir = boost::filesystem::path(location) / "compacted_dir";
    boost::filesystem::create_directory(compacted_dir);
    const boost::filesystem::path compacted = compacted_dir / "pwal_0000.compacted";
    {
        std::ofstream out(compacted.string(), std::ios::binary);
        const std::string bytes = marker(2, 0) + a1 + c0 + e0;
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good());
    }

    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{2, 1024 * 1024, 128}, location);
    const auto result = sorter.scan(10, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(result.max_blob_id, 0U);  // the BLOBs of the compacted WAL file are not counted by the scan
    std::vector<std::string> with_blob;
    std::vector<std::string> output;
    sorter.merge(sorted_file_merger::compacted_wal_input{compacted, [&with_blob](const log_entry_view& view) {
                     with_blob.emplace_back(view.serialized_entry());
                 }},
                 [&output](const log_entry_view& view) { output.emplace_back(view.serialized_entry()); });
    EXPECT_EQ(output, (std::vector<std::string>{a2, c0, d1, e0}));
    EXPECT_EQ(with_blob, (std::vector<std::string>{c0}));
}

// in the compaction mode (with a boundary epoch), the entries of a snippet beyond the boundary are not input, but
// the epoch counts in the maximum
TEST_F(wal_sorter_test, compaction_boundary) {
    const std::string a10 = normal_entry(1, "a", "10", 10, 0);
    const std::string b11 = normal_entry(1, "b", "11", 11, 0);
    write_pwal("pwal_0000", marker(2, 10) + a10 + marker(2, 11) + b11);
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{1, 1024 * 1024, 128}, location);
    const auto result = sorter.scan(10, beyond_boundary_handling::skip, {});
    EXPECT_EQ(result.max_appeared_epoch, 11U);
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{a10}));
}

// only the latest version remains even if the versions of a key are in different files (different threads)
TEST_F(wal_sorter_test, versions_of_a_key_across_threads) {
    std::vector<std::string> versions;
    for (int i = 0; i < 4; ++i) {
        versions.push_back(normal_entry(1, "a", "v" + std::to_string(i), static_cast<std::uint64_t>(i + 1), 0));
        write_pwal("pwal_000" + std::to_string(i), marker(2, static_cast<std::uint64_t>(i + 1)) + versions.back());
    }
    dblog_scan scan{boost::filesystem::path(location)};
    wal_sorter sorter(scan, sort_settings{4, 1024 * 1024, 128}, location);
    (void) sorter.scan(4, beyond_boundary_handling::invalidate, {});
    EXPECT_EQ(merge_output(sorter), (std::vector<std::string>{versions[3]}));
}

// a setting of less than one thread is FATAL
TEST_F(wal_sorter_test, threads_less_than_one_is_fatal) {
    dblog_scan scan{boost::filesystem::path(location)};
    EXPECT_DEATH(wal_sorter(scan, sort_settings{0, 1024, 2}, location),
                 "wal_sorter:wal_sorter the number of threads 0 must be at least 1");
}

}  // namespace limestone::testing
