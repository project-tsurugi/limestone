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
#include <string>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include "locator/locator_format.h"

namespace limestone::testing {

namespace lf = limestone::internal::locator_format;

// ロケーティファイルの名前に .loc を付ける
TEST(locator_format_test, locator_file_name_appends_suffix) {
    EXPECT_EQ(lf::locator_file_name("pwal_0000.compacted.3"), "pwal_0000.compacted.3.loc");
    EXPECT_EQ(lf::locator_file_name("snapshot"), "snapshot.loc");
}

// パスはディレクトリを保ち、名前だけに .loc を付ける
TEST(locator_format_test, locator_file_path_keeps_directory) {
    const boost::filesystem::path locatee("/var/log/dir/pwal_0000.compacted.3");
    EXPECT_EQ(lf::locator_file_path(locatee), boost::filesystem::path("/var/log/dir/pwal_0000.compacted.3.loc"));
}

// .loc で終わる名前だけをロケータファイルの名前と判定する
TEST(locator_format_test, is_locator_file_name_checks_suffix) {
    EXPECT_TRUE(lf::is_locator_file_name("pwal_0000.compacted.3.loc"));
    EXPECT_TRUE(lf::is_locator_file_name("snapshot.loc"));
    EXPECT_TRUE(lf::is_locator_file_name(".loc"));
    EXPECT_FALSE(lf::is_locator_file_name("pwal_0000.compacted.3"));
    EXPECT_FALSE(lf::is_locator_file_name("loc"));
    EXPECT_FALSE(lf::is_locator_file_name("pwal_0000.LOC"));
    EXPECT_FALSE(lf::is_locator_file_name(""));
}

// マジックのバイト列は "LIMELOC" に 0x00 (ヘッダ) と 0x80 (終端) を続けたもの。終端はヘッダの最上位ビットを立てた値
TEST(locator_format_test, magic_values) {
    const std::string header_bytes("LIMELOC\0", 8);
    const std::string footer_bytes("LIMELOC\x80", 8);
    std::uint64_t header_le = 0;
    std::uint64_t footer_le = 0;
    for (int i = 7; i >= 0; --i) {
        header_le = (header_le << 8) | static_cast<unsigned char>(header_bytes[static_cast<std::size_t>(i)]);
        footer_le = (footer_le << 8) | static_cast<unsigned char>(footer_bytes[static_cast<std::size_t>(i)]);
    }
    EXPECT_EQ(lf::header_magic, header_le);
    EXPECT_EQ(lf::footer_magic, footer_le);
    EXPECT_EQ(lf::footer_magic, lf::header_magic | (std::uint64_t{1} << 63U));
    EXPECT_EQ(lf::header_size, 4096U);
    EXPECT_EQ(lf::record_size, 8U);
    EXPECT_EQ(lf::footer_size, 16U);
    EXPECT_EQ(lf::format_version, 1U);
}

}  // namespace limestone::testing
