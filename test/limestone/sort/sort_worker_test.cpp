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
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "sort/sort_worker.h"
#include "sort/sorting_directory.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::log_entry_view;
using limestone::internal::sort_worker;
using limestone::internal::sorting_directory;

using log_entry_bytes::normal_entry;
using log_entry_bytes::remove_entry;

namespace {

/**
 * @brief Parses the bytes of one entry and creates a view.
 * @param bytes the bytes of one entry. Fails the test if parsing does not succeed
 * @return the view
 */
log_entry_view view_of(std::string_view bytes) {
    auto r = log_entry_view::parse(bytes);
    EXPECT_TRUE(r) << "parse failed";
    return r.view();
}

/**
 * @brief Reads the whole contents of a file.
 * @param p the file
 * @return the contents
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

class sort_worker_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sort_worker_test";
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
     * @brief Counts the files in the sorting directory.
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

// finish() writes the contents of the buffer as one sorted file (in key_sid order, duplicates removed)
TEST_F(sort_worker_test, finish_writes_the_buffer_as_one_sorted_file) {
    const std::string a_new = normal_entry(1, "a", "new", 2, 0);
    const std::string a_old = normal_entry(1, "a", "old", 1, 0);
    const std::string b_new = remove_entry(1, "b", 3, 0);
    const std::string b_old = normal_entry(1, "b", "old", 1, 5);
    sort_worker worker(*directory_, 1024 * 1024);
    worker.add(view_of(b_old));
    worker.add(view_of(a_old));
    worker.add(view_of(b_new));
    worker.add(view_of(a_new));
    EXPECT_TRUE(worker.sorted_files().empty());
    worker.finish();
    ASSERT_EQ(worker.sorted_files().size(), 1U);
    EXPECT_EQ(read_file(worker.sorted_files()[0]), a_new + b_new);
    EXPECT_EQ(files_in_directory(), 1U);
}

// when the buffer is full, its contents are written out before the entry is put. The remainder becomes the next
// file at finish()
TEST_F(sort_worker_test, writes_a_file_when_the_buffer_is_full) {
    const std::string e0 = normal_entry(1, "k0", "v", 1, 0);
    const std::string e1 = normal_entry(1, "k1", "v", 1, 0);
    const std::string e2 = normal_entry(1, "k2", "v", 1, 0);
    ASSERT_EQ(e0.size(), e1.size());
    sort_worker worker(*directory_, e0.size() * 2);  // exactly 2 entries
    worker.add(view_of(e2));
    worker.add(view_of(e1));
    EXPECT_TRUE(worker.sorted_files().empty());
    worker.add(view_of(e0));  // full: e1 and e2 are written, then e0 is put
    ASSERT_EQ(worker.sorted_files().size(), 1U);
    EXPECT_EQ(read_file(worker.sorted_files()[0]), e1 + e2);
    worker.finish();
    ASSERT_EQ(worker.sorted_files().size(), 2U);
    EXPECT_EQ(read_file(worker.sorted_files()[1]), e0);
    EXPECT_EQ(files_in_directory(), 2U);
}

// an entry larger than the arena becomes a file of its own, and the contents of the buffer are kept
TEST_F(sort_worker_test, oversized_entry_goes_to_its_own_file) {
    const std::string s0 = normal_entry(1, "k0", "v", 1, 0);
    const std::string s1 = normal_entry(1, "k1", "v", 1, 0);
    const std::size_t arena = s0.size() * 2;
    const std::string big = normal_entry(1, "k9", std::string(arena, 'x'), 1, 0);
    sort_worker worker(*directory_, arena);
    worker.add(view_of(s1));
    worker.add(view_of(big));
    ASSERT_EQ(worker.sorted_files().size(), 1U);
    EXPECT_EQ(read_file(worker.sorted_files()[0]), big);
    worker.add(view_of(s0));
    worker.finish();
    ASSERT_EQ(worker.sorted_files().size(), 2U);
    EXPECT_EQ(read_file(worker.sorted_files()[1]), s0 + s1);
}

// finish() without any entry writes no file
TEST_F(sort_worker_test, finish_without_entries_writes_nothing) {
    sort_worker worker(*directory_, 1024);
    worker.finish();
    EXPECT_TRUE(worker.sorted_files().empty());
    EXPECT_EQ(files_in_directory(), 0U);
}

// a write failure (the place of the output is gone) is thrown as limestone_io_exception
TEST_F(sort_worker_test, write_failure_is_thrown) {
    const std::string e = normal_entry(1, "k", "v", 1, 0);
    sort_worker worker(*directory_, 1024);
    worker.add(view_of(e));
    directory_->remove();
    EXPECT_THROW(worker.finish(), limestone_io_exception);
}

// add() after finish() and a second finish() are FATAL
TEST_F(sort_worker_test, add_after_finish_and_second_finish_are_fatal) {
    const std::string e = normal_entry(1, "k", "v", 1, 0);
    sort_worker worker(*directory_, 1024);
    worker.finish();
    EXPECT_DEATH(worker.add(view_of(e)), "sort_worker:add already finished");
    EXPECT_DEATH(worker.finish(), "sort_worker:finish already finished");
}

}  // namespace limestone::testing
