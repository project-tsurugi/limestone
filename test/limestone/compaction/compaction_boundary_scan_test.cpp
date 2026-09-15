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

#include <fstream>
#include <set>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include "compaction_options.h"
#include "internal.h"
#include "limestone/log/testdata.h"
#include "log_entry.h"
#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using limestone::api::log_entry;
using limestone::internal::compaction_options;

// Verifies the boundary behavior of the compaction scan (its first pass) by calling
// create_compaction_output directly with the boundary epoch set on
// compaction_options: entries of the snippets beyond the boundary must not reach the
// compacted file (the sortdb).
class compaction_boundary_scan_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/compaction_boundary_scan_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directories(from_dir())) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    [[nodiscard]] static boost::filesystem::path from_dir() {
        return boost::filesystem::path(location) / "from";
    }

    [[nodiscard]] static boost::filesystem::path to_dir() {
        return boost::filesystem::path(location) / "to";
    }

    // Reads all physical entries of the file without interpreting snippet validity.
    static std::vector<log_entry> read_raw_entries(const boost::filesystem::path& path) {
        std::vector<log_entry> entries;
        std::ifstream in(path.string(), std::ios::in | std::ios::binary);
        log_entry e;
        while (e.read(in)) {
            entries.push_back(e);
        }
        return entries;
    }

    static std::set<std::string> normal_entry_keys(const std::vector<log_entry>& entries) {
        std::set<std::string> keys;
        for (const log_entry& e : entries) {
            if (e.type() == log_entry::entry_type::normal_entry) {
                std::string k;
                e.key(k);
                keys.insert(k);
            }
        }
        return keys;
    }

    // A snippet of epoch 0x100 (key "1234") followed by one of epoch 0x101 (key "5678").
    static constexpr std::string_view two_snippets =
        "\x02\x00\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x100
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
        "\x03\x00\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x100
        "\x02\x01\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x101
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "5678" "vermajor" "verminor" "5678"  // normal_entry
        "\x03\x01\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x101
        ""sv;
};

// With the boundary epoch set, the entries of the snippets beyond the boundary do
// not reach the compacted file, and the input file is not modified.
TEST_F(compaction_boundary_scan_test, compacted_excludes_entries_beyond_boundary) {
    auto input = from_dir() / "pwal_0000.rotated";
    create_file(input, two_snippets);
    std::string original = read_entire_file(input);

    compaction_options options(from_dir(), to_dir(), 1, {"pwal_0000.rotated"});
    options.set_boundary_epoch(0x100);
    limestone::internal::create_compaction_output(options);

    auto compacted = to_dir() / "pwal_0000.compacted";
    ASSERT_TRUE(boost::filesystem::exists(compacted));
    std::vector<log_entry> entries = read_raw_entries(compacted);
    std::set<std::string> keys = normal_entry_keys(entries);
    EXPECT_NE(keys.find("1234"), keys.end());
    EXPECT_EQ(keys.find("5678"), keys.end());

    // The input file is not modified (the snippet beyond the boundary is not invalidated).
    EXPECT_EQ(read_entire_file(input), original);
}

// When the boundary is at or above the max epoch of the file, every entry reaches
// the compacted file.
TEST_F(compaction_boundary_scan_test, compacted_includes_all_entries_within_boundary) {
    auto input = from_dir() / "pwal_0000.rotated";
    create_file(input, two_snippets);

    compaction_options options(from_dir(), to_dir(), 1, {"pwal_0000.rotated"});
    options.set_boundary_epoch(0x101);
    limestone::internal::create_compaction_output(options);

    auto compacted = to_dir() / "pwal_0000.compacted";
    ASSERT_TRUE(boost::filesystem::exists(compacted));
    std::set<std::string> keys = normal_entry_keys(read_raw_entries(compacted));
    EXPECT_NE(keys.find("1234"), keys.end());
    EXPECT_NE(keys.find("5678"), keys.end());
}

} // namespace limestone::testing
