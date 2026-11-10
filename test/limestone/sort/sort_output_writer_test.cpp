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
#include <cstdint>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_reader.h"
#include "sort/sort_output_writer.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::api::log_entry;
using limestone::internal::locator;
using limestone::internal::log_entry_read_result;
using limestone::internal::log_entry_reader;
using limestone::internal::log_entry_view;
using limestone::internal::sort_output_writer;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;

namespace {

/// the entry type of marker_begin
constexpr std::uint8_t marker_begin_type = 2;

/// the format at startup (snapshot): write_version as it is, remove_entries written, no fsync
const sort_output_writer::format keep_all{log_entry::write_version_handling::keep, true, false};

/// the format of the compaction: the oldest write_version, remove_entries not written, fsync
const sort_output_writer::format compaction{log_entry::write_version_handling::oldest, false, true};

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

class sort_output_writer_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/sort_output_writer_test";
    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }
    void TearDown() override {
        boost::filesystem::remove_all(location);
    }
    static boost::filesystem::path file() {
        return boost::filesystem::path(location) / "output";
    }
    static boost::filesystem::path locator_file() {
        return boost::filesystem::path(location) / "output.loc";
    }
    /**
     * @brief 記録間隔を指定したロケータファイルの出力を作る。
     * @param interval 記録間隔 (バイト)
     * @return ロケータファイルの出力
     */
    static sort_output_writer::locator_output locator_output(std::uint64_t interval) {
        return {locator_file(), interval};
    }
    /**
     * @brief 出力したロケータファイルを読む。終端の大きさは出力したファイルの実際の大きさと照合する。
     * @return 読んだロケータファイル
     */
    static locator read_locator() {
        return {locator_file(), boost::filesystem::file_size(file())};
    }
    /**
     * @brief 出力したファイルの位置から 1 つのエントリを読み、そのバイト列を返す。
     * @param position エントリの開始位置
     * @return エントリのバイト列。読めなければテストを失敗させて空を返す
     */
    static std::string entry_at(std::uint64_t position) {
        log_entry_reader reader(file(), position);
        const log_entry_read_result result = reader.next();
        EXPECT_EQ(result.result(), log_entry_read_result::status::ok) << "at " << position;
        if (result.result() != log_entry_read_result::status::ok) {
            return {};
        }
        return std::string(result.view().serialized_entry());
    }
};

// the output begins with a marker_begin of epoch 0 even without entries. ロケータファイルの出力を指定しなければ
// ロケータファイルは作られない
TEST_F(sort_output_writer_test, empty_output_has_marker_begin) {
    sort_output_writer writer(file(), "test file", keep_all, std::nullopt);
    writer.close();
    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0));
    EXPECT_FALSE(boost::filesystem::exists(locator_file()));
}

// the format writing write_version as it is and keeping remove_entries writes the entries of the three types as they
// are. The BLOB IDs are kept
TEST_F(sort_output_writer_test, keeps_entries_as_they_are) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = normal_with_blob(1, "b", "v", 11, 2, {100, 200});
    const std::string e3 = remove_entry(1, "c", 12, 3);

    sort_output_writer writer(file(), "test file", keep_all, std::nullopt);
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e1 + e2 + e3);
}

// the format not writing remove_entries excludes only the remove_entries
TEST_F(sort_output_writer_test, drops_remove_entry) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = remove_entry(1, "b", 11, 2);
    const std::string e3 = normal_with_blob(1, "c", "v", 12, 3, {100});

    sort_output_writer writer(file(), "test file", {log_entry::write_version_handling::keep, false, false},
                              std::nullopt);
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e1 + e3);
}

// the format of the compaction replaces the write_version with the oldest one (0, 0), excludes remove_entries, and
// closes with fsync. The values and the BLOB IDs are kept
TEST_F(sort_output_writer_test, compaction_format) {
    sort_output_writer writer(file(), "test file", compaction, std::nullopt);
    writer.write(view_of(normal_entry(1, "a", "value", 10, 1)));
    writer.write(view_of(remove_entry(1, "b", 11, 2)));
    writer.write(view_of(normal_with_blob(1, "c", "v", 12, 3, {100, 200})));
    writer.close();

    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + normal_entry(1, "a", "value", 0, 0)
                                     + normal_with_blob(1, "c", "v", 0, 0, {100, 200}));
}

// destruction without close() closes the file, and what has been written remains
TEST_F(sort_output_writer_test, destructor_closes_without_close) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    {
        sort_output_writer writer(file(), "test file", keep_all, std::nullopt);
        writer.write(view_of(e));
    }
    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e);
}

// the file cannot be created in a missing directory: limestone_io_exception with errno and the label
TEST_F(sort_output_writer_test, cannot_create_file) {
    const boost::filesystem::path missing = boost::filesystem::path(location) / "missing" / "output";
    try {
        sort_output_writer writer(missing, "test file", keep_all, std::nullopt);
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), ENOENT);
        EXPECT_NE(std::string(e.what()).find("cannot create test file"), std::string::npos) << e.what();
    }
}

// a non-data entry and write() after close() are FATAL
TEST_F(sort_output_writer_test, contract_violations_are_fatal) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    sort_output_writer writer(file(), "test file", keep_all, std::nullopt);
    EXPECT_DEATH(writer.write(view_of(marker(marker_begin_type, 1))), "not a data entry: marker_begin");
    writer.write(view_of(e));
    writer.close();
    EXPECT_DEATH(writer.write(view_of(e)), "sort_output_writer:write already closed");
}

// a second close() does nothing, with or without fsync (what has been written remains)
TEST_F(sort_output_writer_test, second_close_does_nothing) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    {
        sort_output_writer writer(file(), "test file", keep_all, std::nullopt);
        writer.write(view_of(e));
        writer.close();
        writer.close();
        EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + e);
    }
    {
        sort_output_writer writer(file(), "test file", compaction, std::nullopt);
        writer.write(view_of(e));
        writer.close();
        writer.close();
        EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + normal_entry(1, "a", "v", 0, 0));
    }
}

// ロケータファイルのレコードは、書いたデータ系のエントリの開始位置 (marker_begin を含む、それまでに書いたバイト数)。
// 記録間隔 1 ですべてのエントリが記録され、各位置から読むとそのエントリが得られる。終端は出力したファイルの大きさ
TEST_F(sort_output_writer_test, locator_records_the_positions_of_the_data_entries) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = normal_with_blob(1, "b", "v", 11, 2, {100, 200});
    const std::string e3 = remove_entry(1, "c", 12, 3);

    sort_output_writer writer(file(), "test file", keep_all, locator_output(1));
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    const std::uint64_t p1 = limestone::internal::log_entry_format::marker_size;
    const std::uint64_t p2 = p1 + e1.size();
    const std::uint64_t p3 = p2 + e2.size();
    const locator loc = read_locator();
    EXPECT_EQ(loc.positions(), (std::vector<std::uint64_t>{p1, p2, p3}));
    EXPECT_EQ(loc.locatee_size(), p3 + e3.size());
    EXPECT_EQ(entry_at(p1), e1);
    EXPECT_EQ(entry_at(p2), e2);
    EXPECT_EQ(entry_at(p3), e3);
}

// コンパクションの形式 (remove_entry をファイルに書かない) では、ファイルに書かなかった remove_entry はロケータにも
// 記録されず、その後のエントリの位置もずれない。write_version を最も古いものに置き換えても長さは変わらないので、
// 位置はそのまま
TEST_F(sort_output_writer_test, locator_skips_the_remove_entries_not_written) {
    const std::string e1 = normal_entry(1, "a", "value", 10, 1);
    const std::string e2 = remove_entry(1, "b", 11, 2);
    const std::string e3 = normal_with_blob(1, "c", "v", 12, 3, {100});

    sort_output_writer writer(file(), "test file", compaction, locator_output(1));
    writer.write(view_of(e1));
    writer.write(view_of(e2));
    writer.write(view_of(e3));
    writer.close();

    const std::uint64_t p1 = limestone::internal::log_entry_format::marker_size;
    const std::uint64_t p3 = p1 + e1.size();
    const locator loc = read_locator();
    EXPECT_EQ(loc.positions(), (std::vector<std::uint64_t>{p1, p3}));
    EXPECT_EQ(loc.locatee_size(), p3 + e3.size());
    EXPECT_EQ(entry_at(p1), normal_entry(1, "a", "value", 0, 0));
    EXPECT_EQ(entry_at(p3), normal_with_blob(1, "c", "v", 0, 0, {100}));
}

// 記録間隔に従って記録される。位置はエントリの開始位置であり、閾値に達したエントリだけが記録される
TEST_F(sort_output_writer_test, locator_follows_the_record_interval) {
    const std::string e = normal_entry(1, "a", "v", 1, 1);  // 35 バイト
    ASSERT_EQ(e.size(), 35U);
    // 位置は 9、44、79、114、149。間隔 40 では 9 (閾値 40)、44 (閾値 80)、114 (閾値 120)、149 (閾値 160) が記録される
    sort_output_writer writer(file(), "test file", keep_all, locator_output(40));
    for (int i = 0; i < 5; ++i) {
        writer.write(view_of(e));
    }
    writer.close();

    const locator loc = read_locator();
    EXPECT_EQ(loc.positions(), (std::vector<std::uint64_t>{9, 44, 114, 149}));
    EXPECT_EQ(loc.locatee_size(), 9U + 35U * 5U);
    for (const std::uint64_t p : loc.positions()) {
        EXPECT_EQ(entry_at(p), e);
    }
}

// エントリがなければレコードは 0 個で、終端は marker_begin の大きさ
TEST_F(sort_output_writer_test, locator_of_empty_output_has_no_records) {
    sort_output_writer writer(file(), "test file", compaction, locator_output(1));
    writer.close();
    const locator loc = read_locator();
    EXPECT_EQ(loc.record_count(), 0U);
    EXPECT_EQ(loc.locatee_size(), limestone::internal::log_entry_format::marker_size);
}

// ロケータファイルを作れなければ limestone_io_exception
TEST_F(sort_output_writer_test, cannot_create_locator_file) {
    const boost::filesystem::path missing = boost::filesystem::path(location) / "missing" / "output.loc";
    EXPECT_THROW(sort_output_writer(file(), "test file", keep_all, sort_output_writer::locator_output{missing, 1}),
                 limestone_io_exception);
}

// close() せずに破棄するとロケータファイルは終端のない不完全なまま残り、読むと不正になる
TEST_F(sort_output_writer_test, destructor_leaves_the_locator_file_unfinished) {
    {
        sort_output_writer writer(file(), "test file", keep_all, locator_output(1));
        writer.write(view_of(normal_entry(1, "a", "v", 1, 0)));
    }
    EXPECT_TRUE(boost::filesystem::exists(locator_file()));
    EXPECT_THROW(read_locator(), limestone_exception);
}

// ロケータファイルがあっても 2 回目の close() は何もしない (abort せず、内容は残る)
TEST_F(sort_output_writer_test, second_close_with_locator_does_nothing) {
    const std::string e = normal_entry(1, "a", "v", 1, 0);
    sort_output_writer writer(file(), "test file", compaction, locator_output(1));
    writer.write(view_of(e));
    writer.close();
    writer.close();
    EXPECT_EQ(read_file(file()), marker(marker_begin_type, 0) + normal_entry(1, "a", "v", 0, 0));
    EXPECT_EQ(read_locator().positions(), (std::vector<std::uint64_t>{9}));
}

}  // namespace limestone::testing
