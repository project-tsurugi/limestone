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
#include <unistd.h>

#include <cerrno>
#include <fstream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "sort/sorting_directory.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::sorting_directory;

class sorting_directory_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sorting_directory_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        // cleanup after the tests that drop the permissions
        boost::system::error_code ec;
        boost::filesystem::permissions(location, boost::filesystem::owner_all, ec);
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path parent() {
        return {location};
    }
    /**
     * @brief Returns whether p is directly under the parent and its name begins with the prefix.
     * @param p the path to check
     */
    static bool is_sorting_path(const boost::filesystem::path& p) {
        return p.parent_path() == parent() && p.filename().string().rfind(sorting_directory::name_prefix, 0) == 0;
    }
    /**
     * @brief Creates a non-empty file, creating its parent directory if needed.
     * @param p the file to create
     */
    static void touch(const boost::filesystem::path& p) {
        boost::filesystem::create_directories(p.parent_path());
        std::ofstream out(p.string());
        out << "x";
    }
};

// sorting-XXXXXX is created directly under the parent, and remove() removes it with its contents
TEST_F(sorting_directory_test, create_and_remove) {
    sorting_directory dir(parent());
    const boost::filesystem::path sorting = dir.path();
    EXPECT_TRUE(is_sorting_path(sorting)) << sorting;
    EXPECT_TRUE(boost::filesystem::is_directory(sorting));
    EXPECT_TRUE(boost::filesystem::is_empty(sorting));

    touch(dir.new_file_path());
    touch(sorting / "sub" / "file");
    dir.remove();
    EXPECT_FALSE(boost::filesystem::exists(sorting));
    EXPECT_TRUE(boost::filesystem::is_directory(parent()));
}

// the existing entries of the parent (a remnant of a crash, the place of another instance) are left as they are, and
// a second place under the same parent gets a different name
TEST_F(sorting_directory_test, keeps_existing_entries) {
    touch(parent() / "sorting-stale" / "stale");
    sorting_directory first(parent());
    sorting_directory second(parent());
    EXPECT_NE(first.path(), second.path());
    EXPECT_TRUE(is_sorting_path(second.path())) << second.path();
    EXPECT_TRUE(boost::filesystem::exists(parent() / "sorting-stale" / "stale"));
    first.remove();
    EXPECT_TRUE(boost::filesystem::is_directory(second.path()));
    EXPECT_TRUE(boost::filesystem::exists(parent() / "sorting-stale" / "stale"));
}

// new_file_path() returns a different path directly under the place at each call, and does not create the file
TEST_F(sorting_directory_test, new_file_path_is_unique) {
    sorting_directory dir(parent());
    const auto p1 = dir.new_file_path();
    const auto p2 = dir.new_file_path();
    EXPECT_EQ(p1.parent_path(), dir.path());
    EXPECT_EQ(p2.parent_path(), dir.path());
    EXPECT_NE(p1, p2);
    EXPECT_FALSE(boost::filesystem::exists(p1));
    EXPECT_TRUE(boost::filesystem::is_empty(dir.path()));
}

// numbering from multiple threads gives no duplicate
TEST_F(sorting_directory_test, new_file_path_from_threads) {
    sorting_directory dir(parent());
    constexpr int threads = 8;
    constexpr int per_thread = 1000;
    std::mutex mutex;
    std::set<std::string> paths;
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dir, &mutex, &paths]() {
            std::vector<std::string> mine;
            mine.reserve(per_thread);
            for (int i = 0; i < per_thread; ++i) {
                mine.push_back(dir.new_file_path().string());
            }
            {
                const std::lock_guard<std::mutex> lock(mutex);
                paths.insert(mine.begin(), mine.end());
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    EXPECT_EQ(paths.size(), static_cast<std::size_t>(threads) * per_thread);
}

// destruction without remove() also removes it
TEST_F(sorting_directory_test, destructor_removes) {
    boost::filesystem::path sorting;
    {
        sorting_directory dir(parent());
        sorting = dir.path();
        touch(dir.new_file_path());
    }
    EXPECT_FALSE(boost::filesystem::exists(sorting));
    EXPECT_TRUE(boost::filesystem::is_empty(parent()));
}

// calling remove() twice is FATAL
TEST_F(sorting_directory_test, remove_twice_is_fatal) {
    sorting_directory dir(parent());
    dir.remove();
    EXPECT_DEATH(dir.remove(), "sorting_directory:remove already removed");
}

// a missing parent directory fails the creation: limestone_io_exception with errno
TEST_F(sorting_directory_test, missing_parent) {
    try {
        sorting_directory dir(parent() / "missing");
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
        EXPECT_NE(std::string(e.what()).find("cannot create sorting directory"), std::string::npos) << e.what();
    }
}

// a parent directory that cannot be written fails the creation: limestone_io_exception with errno
TEST_F(sorting_directory_test, cannot_create) {
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }
    boost::filesystem::permissions(parent(), boost::filesystem::owner_read | boost::filesystem::owner_exe);
    try {
        sorting_directory dir(parent());
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EACCES);
        EXPECT_NE(std::string(e.what()).find("cannot create sorting directory"), std::string::npos) << e.what();
    }
}

// a parent directory that cannot be written fails remove(): limestone_io_exception with errno
TEST_F(sorting_directory_test, cannot_remove) {
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }
    sorting_directory dir(parent());
    boost::filesystem::permissions(parent(), boost::filesystem::owner_read | boost::filesystem::owner_exe);
    try {
        dir.remove();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EACCES);
        EXPECT_NE(std::string(e.what()).find("cannot remove sorting directory"), std::string::npos) << e.what();
    }
    boost::filesystem::permissions(parent(), boost::filesystem::owner_all);
    EXPECT_TRUE(boost::filesystem::is_directory(dir.path()));
}

}  // namespace limestone::testing
