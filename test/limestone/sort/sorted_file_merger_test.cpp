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
#include <cerrno>
#include <cstddef>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "sort/sorted_file_merger.h"
#include "sort/sorting_directory.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::api::write_version_type;
using limestone::internal::log_entry_view;
using limestone::internal::range_deletions;
using limestone::internal::sorted_file_merger;
using limestone::internal::sorting_directory;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u8;

class sorted_file_merger_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sorted_file_merger_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
        directory_ = std::make_unique<sorting_directory>(location);
    }
    void TearDown() override {
        directory_.reset();
        boost::filesystem::remove_all(location);
    }
    /**
     * @brief Creates a sorted file of the given entries in the sorting directory.
     * @param entries the bytes of the entries (they must be in sorted order)
     * @return the created file
     */
    boost::filesystem::path sorted_file(const std::vector<std::string>& entries) {
        std::string bytes;
        for (const auto& e : entries) {
            bytes += e;
        }
        return write_file(directory_->new_file_path(), bytes);
    }
    /**
     * @brief Writes bytes to a file.
     * @param p the file
     * @param bytes the contents
     * @return p
     */
    static boost::filesystem::path write_file(const boost::filesystem::path& p, std::string_view bytes) {
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }
    /**
     * @brief Runs a merge and collects the bytes of the output.
     * @param fanin the merge fan-in
     * @param files the sorted files to merge
     * @param compacted the compacted WAL file
     * @param deletions the range deletions
     * @return the bytes of the entries of the output
     */
    using compacted_wal_input = sorted_file_merger::compacted_wal_input;
    std::vector<std::string> merge(std::size_t fanin, const std::vector<boost::filesystem::path>& files,
                                   const std::optional<compacted_wal_input>& compacted = std::nullopt,
                                   const range_deletions& deletions = {}) {
        std::vector<std::string> output;
        sorted_file_merger merger(*directory_, fanin);
        merger.merge(files, compacted, deletions, [&output](const log_entry_view& v) {
            output.emplace_back(v.serialized_entry());
        });
        return output;
    }
    /**
     * @brief Returns the number of files directly under the sorting directory.
     */
    [[nodiscard]] std::size_t files_in_directory() const {
        std::size_t n = 0;
        for (const auto& e : boost::filesystem::directory_iterator(directory_->path())) {
            (void) e;
            ++n;
        }
        return n;
    }

    std::unique_ptr<sorting_directory> directory_;
};

// no input gives no output
TEST_F(sorted_file_merger_test, no_input) {
    EXPECT_TRUE(merge(2, {}).empty());
}

// one input passes through; only the first of the same key_sid (the greatest write_version) remains
TEST_F(sorted_file_merger_test, single_file) {
    const std::string a_new = normal_entry(1, "a", "new", 2, 0);
    const std::string a_old = normal_entry(1, "a", "old", 1, 0);
    const std::string b = remove_entry(1, "b", 1, 0);
    const std::string c = normal_with_blob(1, "c", "v", 1, 0, {7});
    const auto f = sorted_file({a_new, a_old, b, c});
    EXPECT_EQ(merge(2, {f}), (std::vector<std::string>{a_new, b, c}));
}

// multiple inputs are merged in the order of key_sid; for the same key_sid across files the greatest write_version
// remains, and an identical entry in multiple files becomes one
TEST_F(sorted_file_merger_test, multiple_files) {
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string b_f1 = normal_entry(1, "b", "f1", 3, 0);
    const std::string b_f2 = normal_entry(1, "b", "f2", 5, 0);
    const std::string b_f3 = remove_entry(1, "b", 4, 0);
    const std::string c = normal_entry(1, "c", "v", 1, 0);
    const std::string d = normal_entry(1, "d", "v", 1, 0);
    const std::string e = normal_entry(2, "e", "v", 1, 0);
    const auto f1 = sorted_file({a, b_f1, e});
    const auto f2 = sorted_file({b_f2, d});
    const auto f3 = sorted_file({b_f3, c, e});
    EXPECT_EQ(merge(3, {f1, f2, f3}), (std::vector<std::string>{a, b_f2, c, d, e}));
}

// range deletions: the entries of the same storage with a write_version smaller than the maximum are excluded; an
// equal one and another storage remain. They are applied after the removal of duplicates, so a key whose newest
// version is older than the maximum does not appear
TEST_F(sorted_file_merger_test, range_deletion) {
    const std::string s1_a = normal_entry(1, "a", "v", 4, 9);     // (4,9) < (5,0): excluded
    const std::string s1_b = normal_entry(1, "b", "v", 5, 0);     // equal: remains
    const std::string s1_c = remove_entry(1, "c", 6, 0);          // greater: remains
    const std::string s1_d_new = normal_entry(1, "d", "v", 6, 0); // the newest remains
    const std::string s1_d_old = normal_entry(1, "d", "v", 3, 0);
    const std::string s1_e_new = normal_entry(1, "e", "v", 3, 0); // the newest, but older than the maximum: excluded
    const std::string s1_e_old = normal_entry(1, "e", "v", 2, 0);
    const std::string s2_a = normal_entry(2, "a", "v", 1, 0);     // another storage: remains
    const auto f = sorted_file({s1_a, s1_b, s1_c, s1_d_new, s1_d_old, s1_e_new, s1_e_old, s2_a});
    const range_deletions deletions{{1, write_version_type{5, 0}}, {3, write_version_type{9, 0}}};
    EXPECT_EQ(merge(2, {f}, std::nullopt, deletions), (std::vector<std::string>{s1_b, s1_c, s1_d_new, s2_a}));
}

// inputs beyond the fan-in are combined by intermediate merges; the output is the same as a single stage, and the
// inputs used by the intermediate merges are removed
TEST_F(sorted_file_merger_test, multistage) {
    std::vector<boost::filesystem::path> files;
    std::vector<std::string> expected;
    const std::string shared_new = normal_entry(1, "shared", "new", 9, 0);
    for (int i = 0; i < 5; ++i) {
        const std::string key = "k" + std::to_string(i);
        const std::string own = normal_entry(1, key, "v", 1, 0);
        const std::string shared = (i == 2) ? shared_new : normal_entry(1, "shared", "old", i, 0);
        files.push_back(sorted_file({own, shared}));
        expected.push_back(own);
    }
    expected.push_back(shared_new);

    EXPECT_EQ(merge(2, files), expected);
    for (const auto& f : files) {
        EXPECT_FALSE(boost::filesystem::exists(f)) << f;
    }
    // combining 5 files with fan-in 2 creates 3 intermediate outputs, and only the 2 inputs of the final merge remain
    EXPECT_EQ(files_in_directory(), 2U);
}

// the compacted WAL file: added to the inputs of the final merge skipping its marker_begin, and each normal_with_blob
// read from it is recorded (including those removed as duplicates). It counts toward the fan-in, so an intermediate
// merge happens even when the number of sorted files equals the fan-in
TEST_F(sorted_file_merger_test, compacted_wal) {
    const std::string a_sorted = normal_entry(1, "a", "sorted", 2, 0);
    const std::string a_compacted = normal_with_blob(1, "a", "compacted", 1, 0, {11});
    const std::string b_compacted = normal_with_blob(1, "b", "compacted", 1, 0, {22, 33});
    const std::string c_compacted = normal_entry(1, "c", "compacted", 1, 0);
    const std::string d_sorted = remove_entry(1, "d", 1, 0);
    const auto f1 = sorted_file({a_sorted});
    const auto f2 = sorted_file({d_sorted});
    const auto compacted = write_file(boost::filesystem::path(location) / "compacted",
                                      marker(2, 0) + a_compacted + b_compacted + c_compacted);

    std::vector<std::string> recorded;
    sorted_file_merger::compacted_wal_input input{compacted, [&recorded](const log_entry_view& v) {
        recorded.emplace_back(v.serialized_entry());
    }};
    EXPECT_EQ(merge(2, {f1, f2}, input), (std::vector<std::string>{a_sorted, b_compacted, c_compacted, d_sorted}));
    EXPECT_EQ(recorded, (std::vector<std::string>{a_compacted, b_compacted}));
    EXPECT_FALSE(boost::filesystem::exists(f1));
    EXPECT_FALSE(boost::filesystem::exists(f2));
    EXPECT_TRUE(boost::filesystem::exists(compacted));
}

// an empty recording callback is not called
TEST_F(sorted_file_merger_test, compacted_wal_without_callback) {
    const std::string a = normal_with_blob(1, "a", "v", 1, 0, {11});
    const auto compacted = write_file(boost::filesystem::path(location) / "compacted", marker(2, 0) + a);
    sorted_file_merger::compacted_wal_input input{compacted, {}};
    EXPECT_EQ(merge(2, {}, input), (std::vector<std::string>{a}));
}

// a marker_begin at the beginning is skipped, and a file with only a marker_begin is empty
TEST_F(sorted_file_merger_test, leading_marker_begin_is_skipped) {
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const auto with_marker = write_file(directory_->new_file_path(), marker(2, 0) + a);
    const auto only_marker = write_file(directory_->new_file_path(), marker(2, 0));
    EXPECT_EQ(merge(2, {with_marker, only_marker}), (std::vector<std::string>{a}));
}

// an input that does not match the format throws limestone_io_exception without errno: a cut entry, an invalid
// type, a marker_begin not at the beginning, a second marker_begin, a storage entry
TEST_F(sorted_file_merger_test, broken_input_throws) {
    const std::string a = normal_entry(1, "a", "v", 1, 0);
    const std::string head = marker(2, 0);
    struct broken_case {
        std::string bytes;
        std::string what;
        std::size_t offset;
    };
    const std::vector<broken_case> cases{
        {a + a.substr(0, 10), "unexpected EOF", a.size()},
        {a + u8(0x7f), "unknown log_entry type 127", a.size()},
        {a + head + a, "non-data entry marker_begin", a.size()},
        {head + head + a, "non-data entry marker_begin", head.size()},
        {a + storage_entry(7, 1, 1, 0), "non-data entry clear_storage", a.size()},
    };
    for (const auto& c : cases) {
        const auto f = write_file(directory_->new_file_path(), c.bytes);
        try {
            (void) merge(2, {f});
            FAIL() << "no exception: " << c.what;
        } catch (const limestone_io_exception& e) {
            EXPECT_EQ(e.error_code(), 0) << c.what;
            const std::string expected = "merge input is broken (" + c.what + ") at offset " + std::to_string(c.offset);
            EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
        }
    }
}

// a missing input throws limestone_io_exception with errno
TEST_F(sorted_file_merger_test, missing_input_throws) {
    try {
        (void) merge(2, {directory_->new_file_path()});
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
    }
}

// an empty output is FATAL
TEST_F(sorted_file_merger_test, empty_output_is_fatal) {
    sorted_file_merger merger(*directory_, 2);
    EXPECT_DEATH(merger.merge({}, std::nullopt, {}, {}), "sorted_file_merger:merge output must not be empty");
}

// a fan-in below 2 is FATAL
TEST_F(sorted_file_merger_test, fanin_below_two_is_fatal) {
    EXPECT_DEATH(sorted_file_merger(*directory_, 1),
                 "sorted_file_merger:sorted_file_merger merge fanin 1 must be at least 2");
    EXPECT_DEATH(sorted_file_merger(*directory_, 0),
                 "sorted_file_merger:sorted_file_merger merge fanin 0 must be at least 2");
}

}  // namespace limestone::testing
