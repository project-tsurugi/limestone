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
#include <cstdint>
#include <set>
#include <string>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "test_root.h"

#include "compaction_catalog.h"
#include "locator/locator_format.h"
#include "online_compaction.h"

namespace limestone::testing {

using limestone::internal::compaction_catalog;
using limestone::internal::measure_uncompacted_wal_bytes;

class measure_uncompacted_wal_bytes_test : public ::testing::Test {
protected:
    const boost::filesystem::path location{"/tmp/measure_uncompacted_wal_bytes_test"};

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    // Creates a file of the given size
    void create_file(const std::string& name, std::size_t size) const {
        boost::filesystem::ofstream stream(location / name, std::ios::binary);
        stream << std::string(size, 'x');
        ASSERT_TRUE(stream.good());
    }
};

// Returns 0 when there is no file
TEST_F(measure_uncompacted_wal_bytes_test, empty_directory) {
    EXPECT_EQ(measure_uncompacted_wal_bytes(location, {}), 0U);
}

// The unrotated WAL files and the rotated WAL files are counted
TEST_F(measure_uncompacted_wal_bytes_test, counts_unrotated_and_rotated_wal_files) {
    create_file("pwal_0000", 10);
    create_file("pwal_0001", 20);
    create_file("pwal_0000.1700000000000.1", 300);
    create_file("pwal_0001.1700000000001.2", 4000);

    EXPECT_EQ(measure_uncompacted_wal_bytes(location, {}), 4330U);
}

// The rotated WAL files in the detached list are not counted
TEST_F(measure_uncompacted_wal_bytes_test, excludes_detached_wal_files) {
    create_file("pwal_0000", 10);
    create_file("pwal_0000.1700000000000.1", 300);
    create_file("pwal_0001.1700000000001.2", 4000);

    const std::set<std::string> detached{"pwal_0000.1700000000000.1"};
    EXPECT_EQ(measure_uncompacted_wal_bytes(location, detached), 4010U);
}

// The compacted WAL file, the carry file, the locator file, and the files that are not WAL are not counted
TEST_F(measure_uncompacted_wal_bytes_test, excludes_files_other_than_wal_files) {
    create_file("pwal_0000", 10);
    const std::string compacted_generation_0 = compaction_catalog::get_compacted_filename();
    const std::string compacted = compaction_catalog::get_compacted_filename_for_generation(3);
    create_file(compacted_generation_0, 100);
    create_file(compacted, 200);
    create_file(compaction_catalog::get_carry_filename_for_generation(3), 400);
    create_file(limestone::internal::locator_format::locator_file_name(compacted), 800);
    create_file("epoch", 1600);
    create_file("compaction_catalog", 3200);
    create_file("limestone-manifest.json", 6400);
    ASSERT_TRUE(boost::filesystem::create_directory(location / "ctrl"));
    ASSERT_TRUE(boost::filesystem::create_directory(location / "pwal_0009"));

    EXPECT_EQ(measure_uncompacted_wal_bytes(location, {}), 10U);
}

// Returns 0 without throwing when the log directory does not exist
TEST_F(measure_uncompacted_wal_bytes_test, missing_directory) {
    EXPECT_EQ(measure_uncompacted_wal_bytes(location / "missing", {}), 0U);
}

}  // namespace limestone::testing
