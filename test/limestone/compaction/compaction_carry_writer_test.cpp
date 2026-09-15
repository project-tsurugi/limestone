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

#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include <limestone/api/limestone_exception.h>

#include "compaction_carry_writer.h"
#include "limestone/log/testdata.h"
#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using limestone::internal::compaction_carry_writer;

// Tests for the carry writer (the second pass of the compaction scan).
// The input snippets are defined as raw byte sequences and the output is compared
// byte for byte, which is the strongest verification of "copied unmodified".
class compaction_carry_writer_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/compaction_carry_writer_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    [[nodiscard]] boost::filesystem::path path_of(const std::string& name) const {
        return boost::filesystem::path(location) / name;
    }

    // A snippet of epoch 0x100 (marker_begin + normal_entry + marker_end).
    static constexpr std::string_view snippet_100 =
        "\x02\x00\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x100
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
        "\x03\x00\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x100
        ""sv;

    // A snippet of epoch 0x101.
    static constexpr std::string_view snippet_101 =
        "\x02\x01\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x101
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "5678" "vermajor" "verminor" "5678"  // normal_entry
        "\x03\x01\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x101
        ""sv;

    // A snippet of epoch 0x102.
    static constexpr std::string_view snippet_102 =
        "\x02\x02\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x102
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "9abc" "vermajor" "verminor" "9abc"  // normal_entry
        "\x03\x02\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x102
        ""sv;

    // A snippet of epoch 0x101 holding a normal_with_blob entry (two blobs).
    static constexpr std::string_view snippet_101_with_blob =
        "\x02\x01\x01\x00\x00\x00\x00\x00\x00"                                              // marker_begin 0x101
        "\x0a\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "5678" "vermajor" "verminor" "5678"  // normal_with_blob (same shape as normal_entry up to here)
        "\x02\x00\x00\x00" "blobid01" "blobid02"                                                 // blob_count 2 + blob_ids
        "\x03\x01\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x101
        ""sv;

    // An invalidated snippet of epoch 0x101.
    static constexpr std::string_view snippet_101_invalidated =
        "\x06\x01\x01\x00\x00\x00\x00\x00\x00"                                              // marker_invalidated_begin 0x101
        "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "5678" "vermajor" "verminor" "5678"  // normal_entry
        "\x03\x01\x01\x00\x00\x00\x00\x00\x00" "\x01"                                       // marker_end 0x101
        ""sv;
};

// Only the snippets beyond the boundary are copied, byte for byte, unmodified.
TEST_F(compaction_carry_writer_test, copies_only_snippets_beyond_boundary) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_100) + std::string(snippet_101));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    ASSERT_TRUE(boost::filesystem::exists(out));
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101));

    // The input is not modified.
    EXPECT_EQ(read_entire_file(input), std::string(snippet_100) + std::string(snippet_101));
}

// No output file is created when no snippet crosses the boundary.
TEST_F(compaction_carry_writer_test, no_carry_file_when_nothing_beyond_boundary) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_100) + std::string(snippet_101));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x101, out);
    bool created = writer.write({input});

    EXPECT_FALSE(created);
    EXPECT_FALSE(boost::filesystem::exists(out));
}

// Snippets of multiple inputs are concatenated in the input order.
TEST_F(compaction_carry_writer_test, snippets_of_multiple_inputs_are_concatenated_in_order) {
    auto input1 = path_of("pwal_0000.rotated");
    auto input2 = path_of("pwal_0001.rotated");
    create_file(input1, std::string(snippet_100) + std::string(snippet_101));
    create_file(input2, std::string(snippet_102));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input1, input2});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101) + std::string(snippet_102));
}

// An invalidated snippet is not copied even when its epoch is beyond the boundary.
TEST_F(compaction_carry_writer_test, invalidated_snippet_is_not_carried) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_100) + std::string(snippet_101_invalidated) + std::string(snippet_102));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_102));
}

// End-of-snippet detection: when a snippet at or below the boundary follows the
// carried one, the copy is cut off exactly at the next snippet header.
TEST_F(compaction_carry_writer_test, span_ends_at_the_next_snippet_header) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_101) + std::string(snippet_100));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101));
}

// When two snippets to carry are adjacent in one file, both are copied in order
// (the close-then-reopen of a span works on consecutive headers).
TEST_F(compaction_carry_writer_test, consecutive_snippets_beyond_boundary_are_both_carried) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_100) + std::string(snippet_101) + std::string(snippet_102));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101) + std::string(snippet_102));
}

// When the snippets to carry are non-adjacent in one file, the snippet at or below
// the boundary in between does not leak into the output (the two-span path).
TEST_F(compaction_carry_writer_test, non_adjacent_snippets_beyond_boundary_are_carried_separately) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_101) + std::string(snippet_100) + std::string(snippet_102));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101) + std::string(snippet_102));
}

// An invalidated header right after a carried snippet acts as its terminator
// (an invalidated header never opens a span but does participate in ending one).
TEST_F(compaction_carry_writer_test, invalidated_header_terminates_the_preceding_span) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_101) + std::string(snippet_101_invalidated));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101));
}

// A snippet holding a normal_with_blob entry (the only entry type with the
// variable-length blob_ids field) is also copied byte for byte, unmodified.
TEST_F(compaction_carry_writer_test, snippet_with_blob_entry_is_carried_verbatim) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, std::string(snippet_100) + std::string(snippet_101_with_blob));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_TRUE(created);
    EXPECT_EQ(read_entire_file(out), std::string(snippet_101_with_blob));
}

// An empty (zero-byte) input file holds no snippet, so no output is created.
TEST_F(compaction_carry_writer_test, empty_input_file_creates_nothing) {
    auto input = path_of("pwal_0000.rotated");
    create_file(input, ""sv);

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({input});

    EXPECT_FALSE(created);
    EXPECT_FALSE(boost::filesystem::exists(out));
}

// An empty input list creates no output file.
TEST_F(compaction_carry_writer_test, empty_input_list_creates_nothing) {
    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    bool created = writer.write({});

    EXPECT_FALSE(created);
    EXPECT_FALSE(boost::filesystem::exists(out));
}

// A missing input file raises an I/O exception and no output file is created.
TEST_F(compaction_carry_writer_test, throws_on_missing_input_file) {
    auto input = path_of("pwal_0000.rotated");  // intentionally not created
    ASSERT_FALSE(boost::filesystem::exists(input));

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    EXPECT_THROW({ writer.write({input}); }, limestone::api::limestone_io_exception);
    EXPECT_FALSE(boost::filesystem::exists(out));
}

// A broken input (an entry cut off in the middle) raises an exception.
TEST_F(compaction_carry_writer_test, throws_on_broken_input) {
    auto input = path_of("pwal_0000.rotated");
    // Drop the tail of snippet_101, cutting the normal_entry in the middle.
    std::string broken = std::string(snippet_101).substr(0, snippet_101.size() - 15);
    create_file(input, broken);

    auto out = path_of("carry_out");
    compaction_carry_writer writer(0x100, out);
    EXPECT_THROW({ writer.write({input}); }, limestone::api::limestone_exception);
}

} // namespace limestone::testing
