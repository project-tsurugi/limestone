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
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "cursor_impl.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"
#include "region_allocator.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::cursor_impl;
using limestone::internal::locator;
using limestone::internal::locator_writer;
using limestone::internal::region_allocator;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::remove_entry;

/// cursor_impl の protected を呼ぶためのクラス
class cursor_impl_range_testable : public cursor_impl {
public:
    using cursor_impl::cursor_impl;
    using cursor_impl::next;
    using cursor_impl::close;
    using cursor_impl::storage;
    using cursor_impl::key;
    using cursor_impl::value;

    ~cursor_impl_range_testable() override {
        close();
    }
};

class cursor_impl_range_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/cursor_impl_range_test";

    /// cursor が返す 1 件 (storage、key、value)
    using row = std::tuple<std::uint64_t, std::string, std::string>;
    using rows = std::vector<row>;

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    static boost::filesystem::path path(const std::string& name) {
        return boost::filesystem::path(location) / name;
    }

    // 内容を書いたファイルを作り、そのパスを返す
    static boost::filesystem::path write_file(const std::string& name, const std::string& bytes) {
        boost::filesystem::path p = path(name);
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }

    // 内容を書いたファイルとそのロケータ (記録間隔 interval) を作り、locator を返す
    static locator make_file(const std::string& name, const std::string& bytes, std::uint64_t interval) {
        boost::filesystem::path locatee = write_file(name, bytes);
        boost::filesystem::path file = lf::locator_file_path(locatee);
        locator_writer::build_from_file(locatee, file, interval);
        return locator{file, bytes.size()};
    }

    // ストレージ 1 の normal_entry。値でどちらのファイルの出力かを見分ける
    static std::string entry(std::string_view key, std::string_view value) {
        return normal_entry(1, key, value, 7, 1);
    }

    static std::string entries(const std::vector<std::pair<std::string, std::string>>& key_values) {
        std::string bytes{};
        for (const auto& [key, value] : key_values) {
            bytes += entry(key, value);
        }
        return bytes;
    }

    // cursor が返す残りをすべて読む
    static rows read_all(cursor_impl_range_testable& cursor) {
        rows result{};
        while (cursor.next()) {
            std::string key{};
            std::string value{};
            cursor.key(key);
            cursor.value(value);
            result.emplace_back(cursor.storage(), key, value);
        }
        return result;
    }

    // 領域を受け取らずに読む cursor の出力 (比較の基準)
    static rows read_full(const std::optional<boost::filesystem::path>& compacted) {
        if (compacted.has_value()) {
            cursor_impl_range_testable cursor(path("snapshot"), *compacted, std::map<api::storage_id_type, api::write_version_type>{});
            return read_all(cursor);
        }
        cursor_impl_range_testable cursor(path("snapshot"), std::map<api::storage_id_type, api::write_version_type>{});
        return read_all(cursor);
    }

    static std::unique_ptr<cursor_impl_range_testable> make_cursor(const std::optional<boost::filesystem::path>& compacted,
                                                                   const std::shared_ptr<region_allocator>& allocator) {
        return std::make_unique<cursor_impl_range_testable>(path("snapshot"), compacted,
                                                            std::map<api::storage_id_type, api::write_version_type>{},
                                                            allocator);
    }

    // 領域ごとの出力を返す。領域数と同じ本数の cursor を作り、まず各 cursor に 1 件ずつ読ませて領域を 1 つずつ
    // 受け取らせてから、それぞれの残りを読む。cursor i の出力が領域 i の出力になる
    static std::vector<rows> read_by_region(const std::optional<boost::filesystem::path>& compacted,
                                            const std::shared_ptr<region_allocator>& allocator) {
        const std::size_t n = allocator->region_count();
        std::vector<std::unique_ptr<cursor_impl_range_testable>> cursors{};
        std::vector<rows> result(n);
        for (std::size_t i = 0; i < n; ++i) {
            cursors.push_back(make_cursor(compacted, allocator));
            if (cursors[i]->next()) {
                std::string key{};
                std::string value{};
                cursors[i]->key(key);
                cursors[i]->value(value);
                result[i].emplace_back(cursors[i]->storage(), key, value);
            }
        }
        for (std::size_t i = 0; i < n; ++i) {
            rows rest = read_all(*cursors[i]);
            result[i].insert(result[i].end(), rest.begin(), rest.end());
        }
        return result;
    }
};

// 1 本の cursor が領域を続けて読むと、領域を受け取らずに読んだ出力と一致する。R が主のレコード数を割り切る場合と
// 割り切らない場合、領域が 1 つの場合を含む。副ファイルだけにある小さいキー (a00)、同じキーが両方にある場合 (k02、
// スナップショット側が採られる)、副ファイルだけにある途中のキー (k05) を含む
TEST_F(cursor_impl_range_test, one_cursor_reads_all_regions) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k05", "c"}}), 1);
    const rows expected{{1, "a00", "c"}, {1, "k00", "s"}, {1, "k02", "s"}, {1, "k04", "s"}, {1, "k05", "c"}, {1, "k06", "s"}};
    ASSERT_EQ(read_full(path("compacted")), expected);
    for (std::size_t r = 1; r <= 5; ++r) {
        auto allocator = std::make_shared<region_allocator>(snapshot, compacted, r);
        std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
        EXPECT_EQ(read_all(*cursor), expected) << "R = " << r;
        // 残りがなくなった後は false のまま
        EXPECT_FALSE(cursor->next()) << "R = " << r;
    }
}

// 領域ごとの出力。副ファイルの範囲は領域の境界のキーをまたいで重なるが、キーの範囲の判定で 1 回ずつになる
TEST_F(cursor_impl_range_test, output_of_each_region) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k05", "c"}}), 1);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    ASSERT_EQ(allocator->region_count(), 4U);
    const std::vector<rows> expected{
        {{1, "a00", "c"}, {1, "k00", "s"}},  // 領域 0: 開始キーなし、終了キー k02
        {{1, "k02", "s"}},                   // 領域 1: k02 以上 k04 未満。副の範囲は a00 から
        {{1, "k04", "s"}, {1, "k05", "c"}},  // 領域 2: k04 以上 k06 未満。副の範囲は k02 から末尾
        {{1, "k06", "s"}},                   // 領域 3: k06 以上、終了キーなし。副の範囲は k05 から末尾
    };
    EXPECT_EQ(read_by_region(path("compacted"), allocator), expected);
}

// 副ファイルのレコードが 1 つだけなら、どの領域も副ファイルを先頭から末尾まで読む範囲になり、キーの範囲の判定だけで
// 出力が決まる。開始キーと等しいキーは含まれ (k02、k04)、終了キーと等しいキーで読み終える
TEST_F(cursor_impl_range_test, key_equal_to_boundary) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    // 記録間隔を大きくして、レコードを位置 0 の 1 つだけにする
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k04", "c"}, {"k05", "c"}, {"k07", "c"}}), 1000);
    ASSERT_EQ(compacted.record_count(), 1U);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    const std::vector<rows> expected{
        {{1, "a00", "c"}, {1, "k00", "s"}},
        {{1, "k02", "s"}},
        {{1, "k04", "s"}, {1, "k05", "c"}},
        {{1, "k06", "s"}, {1, "k07", "c"}},
    };
    EXPECT_EQ(read_by_region(path("compacted"), allocator), expected);
}

// コンパクション済み WAL ファイルがない場合
TEST_F(cursor_impl_range_test, snapshot_only) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k01", "s"}, {"k02", "s"}, {"k03", "s"}, {"k04", "s"}}), 1);
    const rows expected = read_full(std::nullopt);
    ASSERT_EQ(expected.size(), 5U);
    auto allocator = std::make_shared<region_allocator>(snapshot, std::nullopt, 2);
    ASSERT_EQ(allocator->region_count(), 3U);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(std::nullopt, allocator);
    EXPECT_EQ(read_all(*cursor), expected);

    auto allocator2 = std::make_shared<region_allocator>(snapshot, std::nullopt, 2);
    const std::vector<rows> by_region{
        {{1, "k00", "s"}, {1, "k01", "s"}},
        {{1, "k02", "s"}, {1, "k03", "s"}},
        {{1, "k04", "s"}},
    };
    EXPECT_EQ(read_by_region(std::nullopt, allocator2), by_region);
}

// 主のレコードが 0 個なら、最初の next() から false
TEST_F(cursor_impl_range_test, no_records) {
    locator snapshot = make_file("snapshot", "", 1);
    auto allocator = std::make_shared<region_allocator>(snapshot, std::nullopt, 1);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(std::nullopt, allocator);
    EXPECT_FALSE(cursor->next());
}

// 複数の cursor が領域を分担しても、出力を合わせたものは領域を受け取らずに読んだ出力と一致し、各 cursor の出力は
// キーの昇順。コンパクション済み WAL ファイルが主の場合
TEST_F(cursor_impl_range_test, cursors_share_regions) {
    std::vector<std::pair<std::string, std::string>> snapshot_entries{};
    std::vector<std::pair<std::string, std::string>> compacted_entries{};
    for (int i = 0; i < 30; ++i) {
        const std::string key = "k" + std::to_string(10 + i);
        compacted_entries.emplace_back(key, "c");
        if (i % 3 == 0) {
            snapshot_entries.emplace_back(key, "s");
        }
    }
    locator snapshot = make_file("snapshot", entries(snapshot_entries), 1);
    locator compacted = make_file("compacted", entries(compacted_entries), 1);
    rows expected = read_full(path("compacted"));
    ASSERT_EQ(expected.size(), 30U);

    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 4);
    ASSERT_EQ(allocator->region_count(), 8U);
    std::vector<std::unique_ptr<cursor_impl_range_testable>> cursors{};
    for (int i = 0; i < 3; ++i) {
        cursors.push_back(make_cursor(path("compacted"), allocator));
    }
    // 3 本を少しずつ交互に進める
    rows all{};
    std::vector<rows> each(3);
    bool progressed = true;
    while (progressed) {
        progressed = false;
        for (std::size_t i = 0; i < 3; ++i) {
            for (int k = 0; k < 2 && cursors[i]->next(); ++k) {
                std::string key{};
                std::string value{};
                cursors[i]->key(key);
                cursors[i]->value(value);
                each[i].emplace_back(cursors[i]->storage(), key, value);
                progressed = true;
            }
        }
    }
    for (const rows& r : each) {
        EXPECT_TRUE(std::is_sorted(r.begin(), r.end()));
        all.insert(all.end(), r.begin(), r.end());
    }
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, expected);
}

// 領域をまたいでも、スナップショットの remove_entry がコンパクション済み WAL ファイルの同じキーを打ち消す
TEST_F(cursor_impl_range_test, remove_entry_overrides_compacted) {
    std::string snapshot_bytes = entry("k00", "s") + remove_entry(1, "k02", 7, 1) + entry("k04", "s") + remove_entry(1, "k05", 7, 1);
    locator snapshot = make_file("snapshot", snapshot_bytes, 1);
    locator compacted = make_file("compacted", entries({{"k02", "c"}, {"k05", "c"}}), 1);
    const rows expected{{1, "k00", "s"}, {1, "k04", "s"}};
    ASSERT_EQ(read_full(path("compacted")), expected);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    ASSERT_EQ(allocator->region_count(), 4U);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
    EXPECT_EQ(read_all(*cursor), expected);
}

// 割り当てのプローブが失敗する (レコードの位置にデータ系でないエントリがある) と、next() が limestone_exception を投げる
TEST_F(cursor_impl_range_test, probe_failure_in_next) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}}), 1);
    // 副ファイルのレコード 1 の位置に marker を置く (ロケータは手で書く)
    std::string e0 = entry("k01", "c");
    std::string compacted_bytes = e0 + marker(2, 7) + entry("k03", "c");
    boost::filesystem::path locatee = write_file("compacted", compacted_bytes);
    boost::filesystem::path file = lf::locator_file_path(locatee);
    {
        locator_writer writer(file, 1, false);
        writer.note_entry(0);
        writer.note_entry(e0.size());
        writer.close(compacted_bytes.size());
    }
    locator compacted(file, compacted_bytes.size());
    ASSERT_EQ(compacted.record_count(), 2U);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
    // 領域 0 の副の終了位置の二分探索で marker をプローブする
    try {
        (void) cursor->next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        FAIL() << "unexpected I/O exception: " << e.what();
    } catch (const limestone_exception& e) {
        EXPECT_NE(std::string(e.what()).find("is not a data entry"), std::string::npos) << e.what();
    }
}

// 誤った使い方は abort する: 割り当てが空
TEST_F(cursor_impl_range_test, no_allocator_aborts) {
    (void) make_file("snapshot", entries({{"k00", "s"}}), 1);
    EXPECT_DEATH({
        cursor_impl_range_testable cursor(path("snapshot"), std::nullopt,
                                          std::map<api::storage_id_type, api::write_version_type>{}, nullptr);
    }, "no region allocator");
}

}  // namespace limestone::testing
