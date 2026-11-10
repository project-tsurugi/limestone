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
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"

namespace limestone::testing {

using limestone::api::limestone_io_exception;
using limestone::internal::locator_writer;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::storage_entry;
using log_entry_bytes::u64le;

namespace {

/**
 * @brief ファイルの内容を全部読む。
 * @param p ファイル
 * @return 内容
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/**
 * @brief 期待するロケータファイルのバイト列を作る。
 * @param positions レコードの値
 * @param locatee_size 終端に書くロケーティファイルの大きさ
 * @return バイト列
 */
std::string expected_bytes(const std::vector<std::uint64_t>& positions, std::uint64_t locatee_size) {
    std::string s = std::string("LIMELOC\0", 8) + u64le(lf::format_version);
    s.resize(lf::header_size, '\0');
    for (const std::uint64_t p : positions) {
        s += u64le(p);
    }
    s += u64le(locatee_size) + std::string("LIMELOC\x80", 8);
    return s;
}

/**
 * @brief ファイルにバイト列を書く。
 * @param p ファイル
 * @param bytes 内容
 */
void write_file(const boost::filesystem::path& p, const std::string& bytes) {
    std::ofstream out(p.string(), std::ios::binary);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

class locator_writer_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/locator_writer_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path file() {
        return boost::filesystem::path(location) / "output.loc";
    }
    static boost::filesystem::path locatee() {
        return boost::filesystem::path(location) / "output";
    }
};

// レコードがなくてもヘッダと終端が書かれる。ヘッダはマジック、形式バージョン、0 の予約領域
TEST_F(locator_writer_test, zero_records_have_header_and_footer) {
    locator_writer writer(file(), 100, true);
    writer.close(0);
    EXPECT_EQ(writer.record_count(), 0U);
    const std::string bytes = read_file(file());
    EXPECT_EQ(bytes.size(), lf::header_size + lf::footer_size);
    EXPECT_EQ(bytes, expected_bytes({}, 0));
}

// 閾値に達した位置を記録し、閾値を次の倍数に進める。倍数ちょうどの位置も記録する
TEST_F(locator_writer_test, records_follow_the_interval) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);    // 閾値 0 → 記録、閾値 100
    writer.note_entry(50);
    writer.note_entry(99);
    writer.note_entry(100);  // 記録、閾値 200
    writer.note_entry(150);
    writer.note_entry(250);  // 記録 (200 を越えた最初)、閾値 300
    writer.note_entry(299);
    writer.note_entry(300);  // 記録、閾値 400
    writer.close(400);
    EXPECT_EQ(writer.record_count(), 4U);
    EXPECT_EQ(read_file(file()), expected_bytes({0, 100, 250, 300}, 400));
}

// 最初に受け取った位置が 0 でなくてもレコード 0 になる
TEST_F(locator_writer_test, first_entry_is_record_0) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(9);    // 記録、閾値 100
    writer.note_entry(99);
    writer.note_entry(100);  // 記録
    writer.close(200);
    EXPECT_EQ(read_file(file()), expected_bytes({9, 100}, 200));
}

// 1 つのエントリで複数の倍数を越えても記録は 1 つ。閾値は越えた位置の次の倍数になる
TEST_F(locator_writer_test, crossing_multiple_multiples_records_once) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);     // 記録、閾値 100
    writer.note_entry(1000);  // 記録 (100〜1000 の倍数をまとめて越える)、閾値 1100
    writer.note_entry(1050);
    writer.note_entry(1100);  // 記録
    writer.close(1200);
    EXPECT_EQ(read_file(file()), expected_bytes({0, 1000, 1100}, 1200));
}

// 同じ位置を続けて受け取っても戻りとは扱わない (記録は 1 つ)
TEST_F(locator_writer_test, same_offset_twice_is_allowed) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(0);
    writer.note_entry(0);
    writer.close(10);
    EXPECT_EQ(read_file(file()), expected_bytes({0}, 10));
}

// 位置が戻ると abort する。close の後の note_entry も abort する
TEST_F(locator_writer_test, aborts_on_misuse) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(10);
    EXPECT_DEATH(writer.note_entry(9), "went backward");
    writer.close(20);
    EXPECT_DEATH(writer.note_entry(20), "already closed");
}

// close() に最後の位置より小さいロケーティファイルの大きさを渡すと abort する
TEST_F(locator_writer_test, aborts_when_locatee_size_is_smaller_than_last_offset) {
    locator_writer writer(file(), 100, true);
    writer.note_entry(50);
    EXPECT_DEATH(writer.close(49), "smaller than the last offset");
}

// 記録間隔 0 は abort する
TEST_F(locator_writer_test, aborts_on_zero_interval) {
    EXPECT_DEATH(locator_writer(file(), 0, true), "must not be 0");
}

// 作れない場所に指定すると limestone_io_exception
TEST_F(locator_writer_test, throws_when_file_cannot_be_created) {
    EXPECT_THROW(locator_writer(boost::filesystem::path(location) / "no_such_dir" / "x.loc", 100, true),
                 limestone_io_exception);
}

// build_from_file は、ロケーティファイルのデータ系のエントリの位置を書き出し時と同じ規則で記録する。
// marker とストレージ系のエントリは記録の対象にならないが、位置は進む
TEST_F(locator_writer_test, build_from_file_matches_writing) {
    const std::string m = marker(2, 0);                              // 9 バイト、位置 0
    const std::string e1 = normal_entry(1, "a", "v", 1, 1);          // データ系
    const std::string s1 = storage_entry(8, 1, 1, 1);                // add_storage (対象外)
    const std::string e2 = remove_entry(1, "b", 1, 2);               // データ系
    const std::string e3 = normal_with_blob(1, "c", "v", 1, 3, {7}); // データ系
    const std::string e4 = normal_entry(1, "d", "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv", 1, 4);
    const std::string contents = m + e1 + s1 + e2 + e3 + e4;
    write_file(locatee(), contents);

    const std::uint64_t interval = 40;
    const std::size_t count = locator_writer::build_from_file(locatee(), file(), interval);

    // 同じ位置の列を note_entry に渡した結果と一致する
    const boost::filesystem::path expected_file = boost::filesystem::path(location) / "expected.loc";
    locator_writer writer(expected_file, interval, true);
    std::uint64_t offset = m.size();
    writer.note_entry(offset);
    offset += e1.size() + s1.size();
    writer.note_entry(offset);
    offset += e2.size();
    writer.note_entry(offset);
    offset += e3.size();
    writer.note_entry(offset);
    offset += e4.size();
    writer.close(offset);
    EXPECT_EQ(offset, contents.size());
    EXPECT_EQ(count, writer.record_count());
    EXPECT_EQ(read_file(file()), read_file(expected_file));
    // データ系のエントリは 9、69、99、146 から始まり、間隔 40 ではすべて記録される
    EXPECT_EQ(count, 4U);
    EXPECT_EQ(read_file(file()), expected_bytes({9, 69, 99, 146}, 220));
}

// marker しかないロケーティファイルではレコードは 0 個で、終端の大きさはファイルの大きさ
TEST_F(locator_writer_test, build_from_file_of_marker_only) {
    write_file(locatee(), marker(2, 0));
    EXPECT_EQ(locator_writer::build_from_file(locatee(), file(), 100), 0U);
    EXPECT_EQ(read_file(file()), expected_bytes({}, 9));
}

// ロケーティファイルがエントリの途中で終わっていると limestone_io_exception
TEST_F(locator_writer_test, build_from_file_throws_on_broken_locatee) {
    const std::string e = normal_entry(1, "a", "v", 1, 1);
    write_file(locatee(), marker(2, 0) + e.substr(0, 3));
    EXPECT_THROW(locator_writer::build_from_file(locatee(), file(), 100), limestone_io_exception);
}

// ロケーティファイルがなければ limestone_io_exception
TEST_F(locator_writer_test, build_from_file_throws_when_locatee_is_missing) {
    EXPECT_THROW(locator_writer::build_from_file(locatee(), file(), 100), limestone_io_exception);
}

}  // namespace limestone::testing
