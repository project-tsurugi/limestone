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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>
#include <limestone/api/snapshot.h>

#include "limestone/log/log_entry_bytes.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"
#include "snapshot_impl.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::locator_writer;
using limestone::internal::snapshot_impl;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::remove_entry;
using log_entry_bytes::u64le;

/**
 * get_partitioned_cursors() の総括テスト。手作りのスナップショットファイルとコンパクション済み WAL ファイル (とロケータ
 * ファイル) を置き、snapshot_impl の層で、n 本の cursor の出力を合わせたものが get_cursor() の出力と一致し、各 cursor の
 * 出力がキーの昇順であることを確かめる。
 */
class partitioned_reading_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/partitioned_reading_test";
    static constexpr const char* compacted_name = "pwal_0000.compacted";

    /// cursor が返す 1 件 (storage、key、value)
    using row = std::tuple<std::uint64_t, std::string, std::string>;
    using rows = std::vector<row>;
    using clear_storage_map = std::map<api::storage_id_type, api::write_version_type>;

    /// ファイルに書く 1 件のエントリ (バイト列と、並べ替えに使う serialized key_sid)
    struct source_entry {
        std::string key_sid;
        std::string bytes;
    };

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(boost::filesystem::path(location) / "data");
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    static boost::filesystem::path snapshot_path() {
        return boost::filesystem::path(location) / "data" / "snapshot";
    }

    static boost::filesystem::path compacted_path() {
        return boost::filesystem::path(location) / compacted_name;
    }

    static std::string key_sid(std::uint64_t storage, std::string_view key) {
        return u64le(storage) + std::string(key);
    }

    static source_entry normal(std::uint64_t storage, std::string_view key, std::string_view value, std::uint64_t epoch) {
        return {key_sid(storage, key), normal_entry(storage, key, value, epoch, 0)};
    }

    static source_entry with_blob(std::uint64_t storage, std::string_view key, std::string_view value, std::uint64_t epoch) {
        return {key_sid(storage, key), normal_with_blob(storage, key, value, epoch, 0, {11, 12})};
    }

    static source_entry remove(std::uint64_t storage, std::string_view key, std::uint64_t epoch) {
        return {key_sid(storage, key), remove_entry(storage, key, epoch, 0)};
    }

    // エントリを key_sid の順に並べてファイルの内容にする (ファイルの先頭に marker_begin を置く)
    static std::string file_bytes(std::vector<source_entry> entries) {
        std::stable_sort(entries.begin(), entries.end(),
                         [](const source_entry& a, const source_entry& b) { return a.key_sid < b.key_sid; });
        std::string bytes = marker(2, 1);
        for (const source_entry& e : entries) {
            bytes += e.bytes;
        }
        return bytes;
    }

    static void write_file(const boost::filesystem::path& p, const std::string& bytes) {
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
    }

    // ファイルを書き、そのロケータファイルを記録間隔 interval で作る
    static void write_with_locator(const boost::filesystem::path& p, const std::string& bytes, std::uint64_t interval) {
        write_file(p, bytes);
        locator_writer::build_from_file(p, lf::locator_file_path(p), interval);
    }

    // スナップショットファイルと (あれば) コンパクション済み WAL ファイルを置く
    static void place_files(const std::string& snapshot_bytes, const std::optional<std::string>& compacted_bytes,
                            std::uint64_t interval) {
        write_with_locator(snapshot_path(), snapshot_bytes, interval);
        if (compacted_bytes.has_value()) {
            write_with_locator(compacted_path(), *compacted_bytes, interval);
        }
    }

    static std::optional<std::string> compacted_name_if(bool has_compacted) {
        return has_compacted ? std::optional<std::string>(compacted_name) : std::nullopt;
    }

    static rows read_all(api::cursor& cursor) {
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

    static bool ascending(const rows& r) {
        return std::is_sorted(r.begin(), r.end(), [](const row& a, const row& b) {
            return key_sid(std::get<0>(a), std::get<1>(a)) < key_sid(std::get<0>(b), std::get<1>(b));
        });
    }

    // get_cursor() の出力 (比較の基準)
    static rows read_reference(bool has_compacted, const clear_storage_map& clear_storage) {
        snapshot_impl snapshot(location, clear_storage, compacted_name_if(has_compacted), 1);
        std::unique_ptr<api::cursor> cursor = snapshot.get_cursor();
        return read_all(*cursor);
    }

    // n 本の cursor で読み、各 cursor の出力が昇順であることを確かめ、すべてを合わせて key_sid の順に並べて返す
    static rows read_partitioned(bool has_compacted, const clear_storage_map& clear_storage, std::size_t region_records,
                                 std::size_t n) {
        snapshot_impl snapshot(location, clear_storage, compacted_name_if(has_compacted), region_records);
        std::vector<std::unique_ptr<api::cursor>> cursors = snapshot.get_partitioned_cursors(n);
        EXPECT_EQ(cursors.size(), n);
        rows all{};
        for (std::unique_ptr<api::cursor>& cursor : cursors) {
            EXPECT_NE(cursor, nullptr);
            rows r = read_all(*cursor);
            EXPECT_TRUE(ascending(r)) << "n = " << n << ", R = " << region_records;
            all.insert(all.end(), r.begin(), r.end());
        }
        std::sort(all.begin(), all.end(), [](const row& a, const row& b) {
            return key_sid(std::get<0>(a), std::get<1>(a)) < key_sid(std::get<0>(b), std::get<1>(b));
        });
        return all;
    }

    // 置いたファイルについて、n = 1、2、3、8 と R = 1、2、3 のすべての組で、分割して読んだ出力が get_cursor() の出力と
    // 一致することを確かめる。expected_count は基準の件数の検算 (負なら検算しない)
    static void check_all(bool has_compacted, const clear_storage_map& clear_storage, long expected_count) {
        const rows expected = read_reference(has_compacted, clear_storage);
        if (expected_count >= 0) {
            ASSERT_EQ(expected.size(), static_cast<std::size_t>(expected_count));
        }
        for (std::size_t r : {1, 2, 3}) {
            for (std::size_t n : {1, 2, 3, 8}) {
                EXPECT_EQ(read_partitioned(has_compacted, clear_storage, r, n), expected) << "n = " << n << ", R = " << r;
            }
        }
    }

    static std::string key_of(int i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%04d", i);
        return buf;
    }
};

// 主のレコードが 0 個 (両ファイルにデータエントリがない) なら、全 cursor がすぐ終端
TEST_F(partitioned_reading_test, no_records) {
    place_files(file_bytes({}), file_bytes({}), 1);
    check_all(true, {}, 0);
    place_files(file_bytes({}), std::nullopt, 1);
    check_all(false, {}, 0);
}

// 領域が 1 つ (レコードが R 以下)
TEST_F(partitioned_reading_test, single_region) {
    place_files(file_bytes({normal(1, "a", "s", 1), normal(1, "c", "s", 1)}),
                file_bytes({normal(1, "b", "c", 1), normal(1, "d", "c", 1)}), 1);
    const rows expected = read_reference(true, {});
    ASSERT_EQ(expected.size(), 4U);
    for (std::size_t n : {1, 2, 3, 8}) {
        EXPECT_EQ(read_partitioned(true, {}, 100, n), expected) << "n = " << n;
    }
}

// 先頭の領域に、副ファイルにだけある小さいキーがある。主はスナップショットファイル
TEST_F(partitioned_reading_test, small_keys_only_in_secondary) {
    std::vector<source_entry> snapshot{};
    for (int i = 10; i < 40; ++i) {
        snapshot.push_back(normal(1, key_of(i), "s", 1));
    }
    std::vector<source_entry> compacted{normal(1, "a0", "c", 1), normal(1, "a1", "c", 1), normal(1, key_of(25), "c", 1)};
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    check_all(true, {}, 32);
}

// 副ファイルに、領域の境界のキーと等しいキーがある (記録間隔 1、R = 1 では主のすべてのキーが境界)。同じキーは
// スナップショット側が採られる
TEST_F(partitioned_reading_test, secondary_keys_equal_to_boundaries) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 30; ++i) {
        snapshot.push_back(normal(1, key_of(i), "s", 1));
        if (i % 2 == 0) {
            compacted.push_back(normal(1, key_of(i), "c", 1));
        }
    }
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    const rows expected = read_reference(true, {});
    ASSERT_EQ(expected.size(), 30U);
    for (const row& r : expected) {
        EXPECT_EQ(std::get<2>(r), "s");
    }
    check_all(true, {}, 30);
}

// 主がコンパクション済み WAL ファイル (レコード数が多い)
TEST_F(partitioned_reading_test, compacted_is_primary) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 50; ++i) {
        compacted.push_back(normal(1, key_of(i), "c", 1));
        if (i % 5 == 0) {
            snapshot.push_back(normal(1, key_of(i), "s", 2));
        }
    }
    snapshot.push_back(normal(1, key_of(100), "s", 2));
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    check_all(true, {}, 51);
}

// レコード数が同数 (コンパクション済み WAL ファイルが主)
TEST_F(partitioned_reading_test, same_record_count) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 20; ++i) {
        snapshot.push_back(normal(1, key_of(2 * i), "s", 1));
        compacted.push_back(normal(1, key_of(2 * i + 1), "c", 1));
    }
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    check_all(true, {}, 40);
}

// remove_entry (スナップショットの remove_entry がコンパクション済み WAL ファイルの同じキーを打ち消す)、clear_storage
// (ストレージ 2 の境界より古いエントリは両ファイルで読み飛ばす)、blob 付きのエントリ
TEST_F(partitioned_reading_test, remove_entry_clear_storage_and_blob) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 40; ++i) {
        compacted.push_back(normal(1, key_of(i), "c", 1));
        if (i % 4 == 0) {
            snapshot.push_back(remove(1, key_of(i), 5));
        }
    }
    for (int i = 0; i < 20; ++i) {
        // ストレージ 2: epoch 3 未満は clear_storage で読み飛ばされる。スナップショットは epoch 3、コンパクション済みは
        // 偶数番が epoch 2 (読み飛ばし)、奇数番が epoch 4
        snapshot.push_back(i % 3 == 0 ? with_blob(2, key_of(i), "s", 3) : normal(2, key_of(i), "s", 3));
        compacted.push_back(normal(2, key_of(100 + i), "c", i % 2 == 0 ? 2 : 4));
    }
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    const clear_storage_map clear_storage{{2, api::write_version_type{3, 0}}};
    // ストレージ 1: 40 件のうち 10 件が打ち消される。ストレージ 2: スナップショットの 20 件と、コンパクション済みの奇数番 10 件
    check_all(true, clear_storage, 30 + 20 + 10);
}

// コンパクション済み WAL ファイルがない
TEST_F(partitioned_reading_test, snapshot_only) {
    std::vector<source_entry> snapshot{};
    for (int i = 0; i < 37; ++i) {
        snapshot.push_back(normal(1, key_of(i), "s", 1));
    }
    place_files(file_bytes(snapshot), std::nullopt, 1);
    check_all(false, {}, 37);
}

// 記録間隔がエントリより大きく、レコードがエントリを飛ばして置かれる場合。4 KiB を超えるエントリ (プローブの読み直し) を含む
TEST_F(partitioned_reading_test, sparse_records_and_large_entries) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 60; ++i) {
        const std::string value(i % 7 == 0 ? 5000 : 20, 's');
        snapshot.push_back(normal(1, key_of(i), value, 1));
        if (i % 3 == 1) {
            compacted.push_back(normal(1, key_of(i), std::string(i % 11 == 1 ? 6000 : 10, 'c'), 1));
        }
        if (i % 3 == 2) {
            compacted.push_back(normal(1, key_of(i) + "x", "c", 1));
        }
    }
    place_files(file_bytes(snapshot), file_bytes(compacted), 100);
    check_all(true, {}, 80);
}

// キーの分布を乱数で変えて複数回
TEST_F(partitioned_reading_test, random_distributions) {
    for (unsigned seed = 1; seed <= 6; ++seed) {
        std::mt19937 gen(seed);
        std::uniform_int_distribution<int> key_dist(0, 999);
        std::uniform_int_distribution<int> where_dist(0, 9);
        std::uniform_int_distribution<int> len_dist(0, 200);
        std::set<int> keys{};
        while (keys.size() < 150) {
            keys.insert(key_dist(gen));
        }
        std::vector<source_entry> snapshot{};
        std::vector<source_entry> compacted{};
        std::size_t expected_count = 0;
        for (int k : keys) {
            const int where = where_dist(gen);
            const std::string key = key_of(k);
            if (where < 4) {
                // スナップショットだけ
                snapshot.push_back(normal(1, key, std::string(static_cast<std::size_t>(len_dist(gen)), 's'), 2));
                ++expected_count;
            } else if (where < 7) {
                // コンパクション済みだけ
                compacted.push_back(normal(1, key, std::string(static_cast<std::size_t>(len_dist(gen)), 'c'), 1));
                ++expected_count;
            } else if (where < 9) {
                // 両方 (スナップショット側)
                snapshot.push_back(normal(1, key, "s", 2));
                compacted.push_back(normal(1, key, "c", 1));
                ++expected_count;
            } else {
                // コンパクション済みにあり、スナップショットの remove_entry で打ち消される
                snapshot.push_back(remove(1, key, 2));
                compacted.push_back(normal(1, key, "c", 1));
            }
        }
        const std::uint64_t interval = (seed % 2 == 0) ? 1 : 150;
        place_files(file_bytes(snapshot), file_bytes(compacted), interval);
        check_all(true, {}, static_cast<long>(expected_count));
        if (HasFailure()) {
            FAIL() << "seed " << seed;
        }
    }
}

// 読み込み量: 領域ごとの読み込みが終了位置を越えないこと。プロセスの読み込みバイト数 (/proc/self/io の rchar) の増分が、
// 2 つのファイルの大きさの合計とプローブの分に収まり、領域ごとにバッファの大きさ (16 MiB) を読む量にならないことで確かめる
TEST_F(partitioned_reading_test, read_amount_is_bounded_by_the_ranges) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 4000; ++i) {
        snapshot.push_back(normal(1, key_of(i), std::string(500, 's'), 1));
        if (i % 4 == 0) {
            compacted.push_back(normal(1, key_of(i), std::string(500, 'c'), 1));
        }
    }
    const std::string snapshot_bytes = file_bytes(snapshot);
    const std::string compacted_bytes = file_bytes(compacted);
    // 記録間隔 32 KiB: スナップショット (約 2 MB) のレコードは約 64 個
    place_files(snapshot_bytes, compacted_bytes, 32 * 1024);
    const rows expected = read_reference(true, {});
    ASSERT_EQ(expected.size(), 4000U);

    auto read_rchar = [] {
        std::ifstream in("/proc/self/io");
        std::string name{};
        std::uint64_t value = 0;
        while (in >> name >> value) {
            if (name == "rchar:") {
                return value;
            }
        }
        ADD_FAILURE() << "rchar not found in /proc/self/io";
        return std::uint64_t{0};
    };
    const std::uint64_t before = read_rchar();
    EXPECT_EQ(read_partitioned(true, {}, 1, 4), expected);
    const std::uint64_t after = read_rchar();
    const std::uint64_t read_bytes = after - before;
    // 測定が働いていること (スナップショットファイルは少なくとも 1 回は読まれる)
    EXPECT_GE(read_bytes, snapshot_bytes.size()) << "read " << read_bytes << " bytes";
    // 領域 (約 64 個) ごとに 16 MiB 読む量 (約 1 GiB) にならず、ファイルの合計 (約 2.5 MB) とプローブの分 (領域ごとに
    // 4 KiB × 十数回) に収まる
    EXPECT_LT(read_bytes, snapshot_bytes.size() + compacted_bytes.size() + std::uint64_t{8} * 1024 * 1024)
        << "read " << read_bytes << " bytes";
}

// 旧世代の削除との競合: get_partitioned_cursors() の後にコンパクション済み WAL ファイル (とそのロケータファイル) を削除しても
// 残りの領域を読める
TEST_F(partitioned_reading_test, compacted_file_removed_after_cursors_are_created) {
    std::vector<source_entry> snapshot{};
    std::vector<source_entry> compacted{};
    for (int i = 0; i < 40; ++i) {
        snapshot.push_back(normal(1, key_of(i), "s", 1));
        if (i % 2 == 1) {
            compacted.push_back(normal(1, key_of(i) + "x", "c", 1));
        }
    }
    place_files(file_bytes(snapshot), file_bytes(compacted), 1);
    const rows expected = read_reference(true, {});
    ASSERT_EQ(expected.size(), 60U);

    snapshot_impl snapshot_impl_(location, {}, compacted_name_if(true), 1);
    std::vector<std::unique_ptr<api::cursor>> cursors = snapshot_impl_.get_partitioned_cursors(3);
    boost::filesystem::remove(compacted_path());
    boost::filesystem::remove(lf::locator_file_path(compacted_path()));
    rows all{};
    for (std::unique_ptr<api::cursor>& cursor : cursors) {
        rows r = read_all(*cursor);
        EXPECT_TRUE(ascending(r));
        all.insert(all.end(), r.begin(), r.end());
    }
    std::sort(all.begin(), all.end(), [](const row& a, const row& b) {
        return key_sid(std::get<0>(a), std::get<1>(a)) < key_sid(std::get<0>(b), std::get<1>(b));
    });
    EXPECT_EQ(all, expected);
}

// 不正なロケータファイル: get_partitioned_cursors() が limestone_exception を投げる (get_cursor() はロケータファイルを
// 使わないので読める)
TEST_F(partitioned_reading_test, invalid_locator_file_throws) {
    place_files(file_bytes({normal(1, "a", "s", 1)}), file_bytes({normal(1, "b", "c", 1)}), 1);
    // スナップショットファイルのロケータファイルの先頭 (magic) を壊す
    {
        std::fstream f(lf::locator_file_path(snapshot_path()).string(), std::ios::binary | std::ios::in | std::ios::out);
        f.write("xxxx", 4);
    }
    EXPECT_EQ(read_reference(true, {}).size(), 2U);
    {
        snapshot_impl snapshot(location, {}, compacted_name_if(true), 1);
        EXPECT_THROW((void) snapshot.get_partitioned_cursors(2), limestone_exception);
    }
    // 直して、コンパクション済み WAL ファイルの大きさをロケータファイルの終端の値と食い違わせる
    locator_writer::build_from_file(snapshot_path(), lf::locator_file_path(snapshot_path()), 1);
    {
        std::ofstream f(compacted_path().string(), std::ios::binary | std::ios::app);
        f.write("x", 1);
    }
    {
        snapshot_impl snapshot(location, {}, compacted_name_if(true), 1);
        try {
            (void) snapshot.get_partitioned_cursors(2);
            FAIL() << "no exception";
        } catch (const limestone_io_exception& e) {
            FAIL() << "unexpected I/O exception: " << e.what();
        } catch (const limestone_exception&) {
            // 期待どおり
        }
    }
    // ロケータファイルがない (開けない) のは limestone_io_exception
    boost::filesystem::remove(lf::locator_file_path(compacted_path()));
    {
        snapshot_impl snapshot(location, {}, compacted_name_if(true), 1);
        EXPECT_THROW((void) snapshot.get_partitioned_cursors(2), limestone_io_exception);
    }
}

// cursor の next() の中のプローブの失敗 (レコードの位置にデータ系でないエントリがある): 公開 API の cursor::next() は
// 例外を捕まえて abort する
TEST_F(partitioned_reading_test, probe_failure_in_next_aborts) {
    std::vector<source_entry> snapshot{};
    for (int i = 0; i < 6; ++i) {
        snapshot.push_back(normal(1, key_of(i), "s", 1));
    }
    place_files(file_bytes(snapshot), std::nullopt, 1);
    // コンパクション済み WAL ファイル: レコード 1 の位置に marker を置く (ロケータファイルは手で書く)
    const std::string e0 = normal_entry(1, key_of(1), "c", 1, 0);
    const std::string compacted_bytes = e0 + marker(2, 1) + normal_entry(1, key_of(3), "c", 1, 0);
    write_file(compacted_path(), compacted_bytes);
    {
        locator_writer writer(lf::locator_file_path(compacted_path()), 1, false);
        writer.note_entry(0);
        writer.note_entry(e0.size());
        writer.close(compacted_bytes.size());
    }
    snapshot_impl snapshot_impl_(location, {}, compacted_name_if(true), 1);
    std::vector<std::unique_ptr<api::cursor>> cursors = snapshot_impl_.get_partitioned_cursors(2);
    EXPECT_DEATH((void) cursors[0]->next(), "is not a data entry");
}

}  // namespace limestone::testing
