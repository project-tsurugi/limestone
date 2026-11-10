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
#include <cerrno>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "file_operations.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::locator;
using limestone::internal::file_operations;
using limestone::internal::locator_writer;
using limestone::internal::real_file_operations;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::u64le;

namespace {

/**
 * @brief ファイルの内容を全部読む。
 */
std::string read_file(const boost::filesystem::path& p) {
    std::ifstream in(p.string(), std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "cannot open " << p;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/**
 * @brief ファイルにバイト列を書く。
 */
void write_file(const boost::filesystem::path& p, const std::string& bytes) {
    std::ofstream out(p.string(), std::ios::binary);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

/**
 * @brief ロケータファイルが不正と判定されることを確かめる。I/O の例外ではなく、メッセージに expected を含む
 *        limestone_exception が投げられること
 * @param file ロケータファイル
 * @param locatee_size locator に与えるロケーティファイルの大きさ
 * @param ops locator に与えるファイル操作
 * @param expected メッセージに含まれるべき部分文字列
 */
void expect_invalid(const boost::filesystem::path& file, std::uint64_t locatee_size, file_operations& ops,
                    const std::string& expected) {
    try {
        locator loc(file, locatee_size, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        FAIL() << "unexpected I/O exception: " << e.what();
    } catch (const limestone_exception& e) {
        EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
    }
}

/**
 * @brief 実際のファイル操作で expect_invalid() を行う。
 */
void expect_invalid(const boost::filesystem::path& file, std::uint64_t locatee_size, const std::string& expected) {
    real_file_operations ops{};
    expect_invalid(file, locatee_size, ops, expected);
}

}  // namespace

class locator_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/locator_test";
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
    /**
     * @brief locator_writer で正しいロケータファイルを作り、その内容を返す。
     * @param positions レコードの値
     * @param locatee_size 終端に書くロケーティファイルの大きさ
     * @return 書いたバイト列
     */
    static std::string write_valid(const std::vector<std::uint64_t>& positions, std::uint64_t locatee_size) {
        locator_writer writer(file(), 1, true);
        for (const std::uint64_t p : positions) {
            writer.note_entry(p);
        }
        writer.close(locatee_size);
        return read_file(file());
    }
};

// locator_writer が書いたものを読み戻せる
TEST_F(locator_test, reads_what_writer_wrote) {
    write_valid({0, 100, 250}, 400);
    locator loc(file(), 400);
    EXPECT_EQ(loc.record_count(), 3U);
    EXPECT_EQ(loc.positions(), (std::vector<std::uint64_t>{0, 100, 250}));
    EXPECT_EQ(loc.position(0), 0U);
    EXPECT_EQ(loc.position(1), 100U);
    EXPECT_EQ(loc.position(2), 250U);
    EXPECT_EQ(loc.locatee_size(), 400U);
    EXPECT_EQ(loc.file(), file());
}

// レコード 0 個のロケータファイルも読める
TEST_F(locator_test, reads_zero_records) {
    write_valid({}, 9);
    locator loc(file(), 9);
    EXPECT_EQ(loc.record_count(), 0U);
    EXPECT_TRUE(loc.positions().empty());
    EXPECT_EQ(loc.locatee_size(), 9U);
}

// 範囲外のレコード番号は abort する
TEST_F(locator_test, position_out_of_range_aborts) {
    write_valid({0}, 9);
    locator loc(file(), 9);
    EXPECT_DEATH(static_cast<void>(loc.position(1)), "out of range");
}

// ヘッダのマジックが一致しないと不正
TEST_F(locator_test, rejects_wrong_header_magic) {
    std::string bytes = write_valid({0}, 9);
    bytes[0] = 'X';
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the magic of the header");
}

// ヘッダの形式バージョンが 1 でないと不正
TEST_F(locator_test, rejects_wrong_format_version) {
    std::string bytes = write_valid({0}, 9);
    bytes.replace(8, 8, u64le(2));
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the format version 2 is not 1");
}

// 大きさからヘッダと終端を引いた値が 8 で割り切れないと不正
TEST_F(locator_test, rejects_size_not_multiple_of_record) {
    std::string bytes = write_valid({0, 100}, 200);
    bytes.insert(lf::header_size, "\0", 1);  // レコードの列に 1 バイト挿入
    write_file(file(), bytes);
    expect_invalid(file(), 200, "is not header + records + footer");
}

// ヘッダと終端の長さに満たないと不正
TEST_F(locator_test, rejects_too_short_file) {
    const std::string bytes = write_valid({}, 9);
    write_file(file(), bytes.substr(0, bytes.size() - 8));
    expect_invalid(file(), 9, "is not header + records + footer");
    write_file(file(), "");
    expect_invalid(file(), 9, "shorter than the header");
}

// 終端のマジックが一致しないと不正 (書き途中で止まった形)
TEST_F(locator_test, rejects_wrong_footer_magic) {
    std::string bytes = write_valid({0}, 9);
    bytes.back() = '\0';
    write_file(file(), bytes);
    expect_invalid(file(), 9, "the magic of the footer");
}

// 終端が丸ごとなく、大きさの条件だけは満たす形 (レコード 2 個分が終端に見える) も不正
TEST_F(locator_test, rejects_missing_footer) {
    std::string bytes = write_valid({0, 100}, 200);
    bytes.resize(bytes.size() - lf::footer_size);
    write_file(file(), bytes);
    expect_invalid(file(), 200, "the magic of the footer");
}

// 終端のロケーティファイルの大きさが与えた大きさと一致しないと不正
TEST_F(locator_test, rejects_locatee_size_mismatch) {
    write_valid({0}, 9);
    expect_invalid(file(), 10, "the locatee size 9 in the footer does not match the actual size 10");
    EXPECT_NO_THROW(locator(file(), 9));
}

// ファイルがなければ limestone_io_exception
TEST_F(locator_test, throws_when_file_is_missing) {
    EXPECT_THROW(locator(file(), 0), limestone_io_exception);
}

// ディレクトリなら開けるが読めないので limestone_io_exception (ヘッダの読み込みの誤り)
TEST_F(locator_test, throws_when_file_is_a_directory) {
    try {
        locator loc(location, 0);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EISDIR);
    }
}

// fstat が失敗すれば errno 付きの limestone_io_exception
TEST_F(locator_test, throws_when_fstat_fails) {
    class : public real_file_operations {
    public:
        int fstat(int, struct stat*) override {
            errno = EIO;
            return -1;
        }
    } ops;
    write_valid({0}, 9);
    try {
        locator loc(file(), 9, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// ヘッダの後の読み込みが誤りになれば errno 付きの limestone_io_exception
TEST_F(locator_test, throws_when_reading_after_the_header_fails) {
    class : public real_file_operations {
    public:
        size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++calls_ == 1) {
                return real_file_operations::fread(ptr, size, count, stream);
            }
            errno = EIO;
            return 0;
        }
        int ferror(FILE*) override { return 1; }
    private:
        int calls_{};
    } ops;
    write_valid({0}, 9);
    try {
        locator loc(file(), 9, ops);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
    }
}

// fstat の後にファイルが縮めば (読み込みが短く終わり誤りでもなければ) limestone_exception
TEST_F(locator_test, rejects_file_shrunk_after_fstat) {
    class : public real_file_operations {
    public:
        size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++calls_ == 1) {
                return real_file_operations::fread(ptr, size, count, stream);
            }
            return 0;
        }
    private:
        int calls_{};
    } ops;
    write_valid({0}, 9);
    expect_invalid(file(), 9, ops, "shorter than its size");
}

}  // namespace limestone::testing
