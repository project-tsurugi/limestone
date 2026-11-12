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
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "file_operations.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"
#include "log_entry/log_entry_reader.h"
#include "region_allocator.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::locator;
using limestone::internal::locator_writer;
using limestone::internal::log_entry_reader;
using limestone::internal::real_file_operations;
using limestone::internal::region_allocator;
using limestone::internal::region_range;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::u64le;
using log_entry_bytes::u8;

namespace {

// pread の要求した大きさを記録するファイル操作
class recording_file_operations : public real_file_operations {
public:
    ssize_t pread(int fd, void* buf, size_t count, off_t offset) override {
        requests_.push_back(count);
        return real_file_operations::pread(fd, buf, count, offset);
    }
    [[nodiscard]] const std::vector<std::size_t>& requests() const noexcept { return requests_; }
    void clear() { requests_.clear(); }

private:
    std::vector<std::size_t> requests_{};
};

}  // namespace

class region_allocator_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/region_allocator_test";
    /// 1 つのエントリ (entry() で作る) の大きさ
    static constexpr std::uint64_t entry_size = 38;

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    // 大きさが entry_size の normal_entry (キーは 3 文字)
    static std::string entry(std::string_view key) {
        return normal_entry(1, key, "vv", 7, 1);
    }

    // entry() の serialized key_sid
    static std::string key_sid(std::string_view key) {
        return u64le(1) + std::string(key);
    }

    // キーの列から、entry() を並べたファイルの内容を作る
    static std::string entries(const std::vector<std::string>& keys) {
        std::string bytes{};
        for (const std::string& key : keys) {
            bytes += entry(key);
        }
        return bytes;
    }

    // 内容を書いたファイルを作り、そのパスを返す
    static boost::filesystem::path write_file(const std::string& name, const std::string& bytes) {
        boost::filesystem::path p = boost::filesystem::path(location) / name;
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }

    // ファイルのロケータファイルを記録間隔 interval で作り、読んだ locator を返す
    static locator make_locator(const boost::filesystem::path& locatee, std::uint64_t interval) {
        const boost::filesystem::path file = lf::locator_file_path(locatee);
        locator_writer::build_from_file(locatee, file, interval);
        return locator{file, boost::filesystem::file_size(locatee)};
    }

    // 内容を書いたファイルとそのロケータ (記録間隔 interval) を作り、locator を返す
    static locator make_file(const std::string& name, const std::string& bytes, std::uint64_t interval) {
        return make_locator(write_file(name, bytes), interval);
    }

    static boost::filesystem::path path(const std::string& name) {
        return boost::filesystem::path(location) / name;
    }

    static void expect_key(const std::optional<std::string>& actual, const std::optional<std::string>& expected) {
        ASSERT_EQ(actual.has_value(), expected.has_value());
        if (expected.has_value()) {
            EXPECT_EQ(*actual, *expected);
        }
    }

    // limestone_exception (limestone_io_exception ではない) が投げられ、メッセージが expected を含むこと
    template <class F>
    static void expect_throws(F&& f, const std::string& expected) {
        try {
            f();
            FAIL() << "no exception";
        } catch (const limestone_io_exception& e) {
            FAIL() << "unexpected I/O exception: " << e.what();
        } catch (const limestone_exception& e) {
            EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
        }
    }
};

// レコード数の多いスナップショットファイルが主。領域ごとに、主の開始位置と終了位置、開始キーと終了キー、副の開始位置と
// 終了位置が求まる。先頭の領域に開始キーはなく、最後の領域に終了キーはない (レコード数 10 は R = 4 で割り切れない)
TEST_F(region_allocator_test, snapshot_is_primary) {
    // スナップショット: k00..k09 の 10 件、全エントリがレコード。コンパクション済み: k01, k03, k05, k07 の 4 件
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k03", "k05", "k07"}), 1);
    ASSERT_EQ(snapshot.record_count(), 10U);
    ASSERT_EQ(compacted.record_count(), 4U);
    region_allocator allocator(snapshot, compacted, 4);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    // 領域 0: k00..k03。副は先頭から、k04 以上の最初のレコード (k05、2 番) まで
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 4 * entry_size);
    expect_key(r0->start_key, std::nullopt);
    expect_key(r0->end_key, key_sid("k04"));
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 2 * entry_size);

    // 領域 1: k04..k07。副は k04 より小さい最後のレコード (k03、1 番) から、k08 以上のレコードはないので末尾まで
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, 4 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 8 * entry_size);
    expect_key(r1->start_key, key_sid("k04"));
    expect_key(r1->end_key, key_sid("k08"));
    EXPECT_EQ(r1->compacted_start, 1 * entry_size);
    EXPECT_EQ(r1->compacted_end, 4 * entry_size);

    // 領域 2 (最後): k08..k09。主の終了位置は末尾。副は k08 より小さい最後のレコード (k07、3 番) から末尾まで
    std::optional<region_range> r2 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->snapshot_start, 8 * entry_size);
    EXPECT_EQ(r2->snapshot_end, 10 * entry_size);
    expect_key(r2->start_key, key_sid("k08"));
    expect_key(r2->end_key, std::nullopt);
    EXPECT_EQ(r2->compacted_start, 3 * entry_size);
    EXPECT_EQ(r2->compacted_end, 4 * entry_size);

    // 残りはない
    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
}

// レコード数の多いコンパクション済み WAL ファイルが主。範囲の値はスナップショットとコンパクション済みの言葉で返る
TEST_F(region_allocator_test, compacted_is_primary) {
    locator snapshot = make_file("snapshot", entries({"k01", "k03", "k05", "k07"}), 1);
    locator compacted = make_file("compacted", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    region_allocator allocator(snapshot, compacted, 4);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 4 * entry_size);
    expect_key(r0->start_key, std::nullopt);
    expect_key(r0->end_key, key_sid("k04"));
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 2 * entry_size);

    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 4 * entry_size);
    EXPECT_EQ(r1->compacted_end, 8 * entry_size);
    expect_key(r1->start_key, key_sid("k04"));
    expect_key(r1->end_key, key_sid("k08"));
    EXPECT_EQ(r1->snapshot_start, 1 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 4 * entry_size);

    std::optional<region_range> r2 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->compacted_start, 8 * entry_size);
    EXPECT_EQ(r2->compacted_end, 10 * entry_size);
    expect_key(r2->start_key, key_sid("k08"));
    expect_key(r2->end_key, std::nullopt);
    EXPECT_EQ(r2->snapshot_start, 3 * entry_size);
    EXPECT_EQ(r2->snapshot_end, 4 * entry_size);

    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
}

// レコード数が同数ならコンパクション済み WAL ファイルが主
TEST_F(region_allocator_test, same_record_count_compacted_is_primary) {
    locator snapshot = make_file("snapshot", entries({"k00", "k02", "k04", "k06"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k03", "k05", "k07"}), 1);
    region_allocator allocator(snapshot, compacted, 2);
    EXPECT_EQ(allocator.region_count(), 2U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    // 領域 0: 主 (コンパクション済み) は k01, k03。終了キー k05。副は k05 以上の最初のレコード (k06、3 番) まで
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 2 * entry_size);
    expect_key(r0->end_key, key_sid("k05"));
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 3 * entry_size);

    // 領域 1: 開始キー k05。副は k05 より小さい最後のレコード (k04、2 番) から末尾まで
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 2 * entry_size);
    EXPECT_EQ(r1->compacted_end, 4 * entry_size);
    expect_key(r1->start_key, key_sid("k05"));
    expect_key(r1->end_key, std::nullopt);
    EXPECT_EQ(r1->snapshot_start, 2 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 4 * entry_size);
}

// コンパクション済み WAL ファイルがなければスナップショットファイルが主で副はなく、その範囲は 0, 0
TEST_F(region_allocator_test, no_compacted_file) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 3);
    EXPECT_EQ(allocator.region_count(), 4U);
    log_entry_reader snapshot_reader(path("snapshot"));

    for (std::size_t j = 0; j < 4; ++j) {
        std::optional<region_range> r = allocator.allocate(snapshot_reader, nullptr);
        ASSERT_TRUE(r.has_value()) << j;
        EXPECT_EQ(r->snapshot_start, 3 * j * entry_size) << j;
        EXPECT_EQ(r->snapshot_end, std::min<std::uint64_t>(3 * (j + 1), 10) * entry_size) << j;
        EXPECT_EQ(r->compacted_start, 0U) << j;
        EXPECT_EQ(r->compacted_end, 0U) << j;
        expect_key(r->start_key, j == 0 ? std::nullopt : std::optional<std::string>(key_sid("k0" + std::to_string(3 * j))));
        expect_key(r->end_key, j == 3 ? std::nullopt : std::optional<std::string>(key_sid("k0" + std::to_string(3 * (j + 1)))));
    }
    EXPECT_FALSE(allocator.allocate(snapshot_reader, nullptr).has_value());
}

// R = 1 では領域数がレコード数と同じ
TEST_F(region_allocator_test, one_record_per_region) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 1);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, entry_size);
    EXPECT_EQ(r1->snapshot_end, 2 * entry_size);
    expect_key(r1->start_key, key_sid("k01"));
    expect_key(r1->end_key, key_sid("k02"));
}

// 記録間隔がエントリより大きければ、レコードはエントリを飛ばして置かれる。領域の位置はレコードの位置に従う
TEST_F(region_allocator_test, record_interval_larger_than_entry) {
    // 記録間隔 100 バイト: 位置 0、114 (3 番目)、228 (6 番目)、342 (9 番目) がレコード
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 100);
    ASSERT_EQ(snapshot.record_count(), 4U);
    region_allocator allocator(snapshot, std::nullopt, 2);
    EXPECT_EQ(allocator.region_count(), 2U);
    log_entry_reader snapshot_reader(path("snapshot"));
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 6 * entry_size);
    expect_key(r0->end_key, key_sid("k06"));
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, 6 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 10 * entry_size);
    expect_key(r1->start_key, key_sid("k06"));
    expect_key(r1->end_key, std::nullopt);
}

// 主のレコードが 0 個なら領域はなく、最初の allocate() から残りがない
TEST_F(region_allocator_test, no_records) {
    locator snapshot = make_file("snapshot", "", 1);
    ASSERT_EQ(snapshot.record_count(), 0U);
    region_allocator allocator(snapshot, std::nullopt, 1);
    EXPECT_EQ(allocator.region_count(), 0U);
    log_entry_reader snapshot_reader(path("snapshot"));
    EXPECT_FALSE(allocator.allocate(snapshot_reader, nullptr).has_value());
}

// 副のキーがすべて開始キー以上なら副の開始位置は 0。副のキーがすべて終了キーより小さければ副の終了位置は末尾
TEST_F(region_allocator_test, secondary_without_matching_record) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 2);
    log_entry_reader snapshot_reader(path("snapshot"));
    {
        // 副のキーはすべて領域 1 の開始キー k02 以上、領域 1 の終了キー k04 以上の最初のレコードは 0 番
        locator compacted = make_file("compacted", entries({"k07", "k08"}), 1);
        region_allocator a(snapshot, compacted, 2);
        log_entry_reader compacted_reader(path("compacted"));
        (void) a.allocate(snapshot_reader, &compacted_reader);
        std::optional<region_range> r1 = a.allocate(snapshot_reader, &compacted_reader);
        ASSERT_TRUE(r1.has_value());
        expect_key(r1->start_key, key_sid("k02"));
        expect_key(r1->end_key, key_sid("k04"));
        EXPECT_EQ(r1->compacted_start, 0U);
        EXPECT_EQ(r1->compacted_end, 0U);
    }
    {
        // 副のキーはすべて領域 1 の終了キー k04 より小さい。開始キー k02 より小さい最後のレコードは 1 番
        locator compacted = make_file("compacted2", entries({"a00", "a01"}), 1);
        region_allocator a(snapshot, compacted, 2);
        log_entry_reader compacted_reader(path("compacted2"));
        (void) a.allocate(snapshot_reader, &compacted_reader);
        std::optional<region_range> r1 = a.allocate(snapshot_reader, &compacted_reader);
        ASSERT_TRUE(r1.has_value());
        EXPECT_EQ(r1->compacted_start, 1 * entry_size);
        EXPECT_EQ(r1->compacted_end, 2 * entry_size);
    }
}

// 副のレコードのキーが境界のキーと等しいとき: 開始位置は「開始キーより小さい最後」なので、等しいレコードの 1 つ前。
// 終了位置は「終了キー以上の最初」なので、等しいレコードの位置
TEST_F(region_allocator_test, secondary_record_equal_to_boundary_key) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k02", "k03", "k04"}), 1);
    region_allocator allocator(snapshot, compacted, 2);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));
    (void) allocator.allocate(snapshot_reader, &compacted_reader);
    // 領域 1: 開始キー k02、終了キー k04。副の開始は k01 (0 番)、終了は k04 (3 番) の位置
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 0U);
    EXPECT_EQ(r1->compacted_end, 3 * entry_size);
}

// 複数のスレッドから同時に allocate() しても、各領域はちょうど 1 回だけ配られる
TEST_F(region_allocator_test, allocate_concurrently) {
    std::vector<std::string> keys{};
    for (int i = 0; i < 40; ++i) {
        keys.push_back("k" + std::to_string(10 + i));
    }
    locator snapshot = make_file("snapshot", entries(keys), 1);
    locator compacted = make_file("compacted", entries({"k15", "k25", "k35"}), 1);
    region_allocator allocator(snapshot, compacted, 3);
    ASSERT_EQ(allocator.region_count(), 14U);

    constexpr int thread_count = 4;
    std::vector<std::vector<std::uint64_t>> starts(thread_count);
    std::vector<std::thread> threads{};
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([&, t] {
            log_entry_reader snapshot_reader(path("snapshot"));
            log_entry_reader compacted_reader(path("compacted"));
            while (true) {
                std::optional<region_range> r = allocator.allocate(snapshot_reader, &compacted_reader);
                if (! r.has_value()) {
                    break;
                }
                starts[t].push_back(r->snapshot_start);
            }
        });
    }
    for (std::thread& th : threads) {
        th.join();
    }
    std::multiset<std::uint64_t> all{};
    for (const std::vector<std::uint64_t>& s : starts) {
        all.insert(s.begin(), s.end());
    }
    std::multiset<std::uint64_t> expected{};
    for (std::size_t j = 0; j < 14; ++j) {
        expected.insert(3 * j * entry_size);
    }
    EXPECT_EQ(all, expected);
}

// プローブは、エントリが 4 KiB に収まれば pread 1 回 (4 KiB か、ファイルの残りの小さいほう)
TEST_F(region_allocator_test, probe_small_entry) {
    std::string bytes = entries({"k00", "k01", "k02"});
    boost::filesystem::path p = write_file("snapshot", bytes);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader, entry_size, bytes.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{2 * entry_size}));
    ops.clear();
    std::string large = bytes + std::string(8192, '\0');  // 末尾のごみは読まれない
    p = write_file("snapshot2", large);
    log_entry_reader reader2(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader2, entry_size, large.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096}));
}

// 4 KiB を超えるエントリは、4 KiB の次に必要な分を読む。normal_with_blob はもう 1 段ある
TEST_F(region_allocator_test, probe_large_entry) {
    std::string e0 = entry("k00");
    std::string e1 = normal_entry(1, "k01", std::string(10000, 'v'), 7, 1);
    std::string e2 = normal_with_blob(1, "k02", std::string(10000, 'v'), 7, 1, {11, 12});
    std::string bytes = e0 + e1 + e2;
    boost::filesystem::path p = write_file("snapshot", bytes);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader, e0.size(), bytes.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096, e1.size()}));
    ops.clear();
    EXPECT_EQ(region_allocator::probe(reader, e0.size() + e1.size(), bytes.size()), key_sid("k02"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096, e2.size() - 2 * 8, e2.size()}));
}

// プローブの失敗: データ系でないエントリ、不正な種類、位置がファイルの大きさ以上
TEST_F(region_allocator_test, probe_fails_on_invalid_entry) {
    std::string e0 = entry("k00");
    std::string bytes = e0 + marker(2, 7) + u8(99);
    boost::filesystem::path p = write_file("snapshot", bytes);
    log_entry_reader reader(p);
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), bytes.size()); }, "is not a data entry");
    expect_throws([&] { (void) region_allocator::probe(reader, bytes.size() - 1, bytes.size()); }, "is broken");
    expect_throws([&] { (void) region_allocator::probe(reader, bytes.size(), bytes.size()); }, "is not within the file size");
}

// プローブの失敗: ファイルがロケータの終端の値より小さい (エントリが途中で切れている、位置の先にデータがない)。
// 繰り返しにならずに失敗する
TEST_F(region_allocator_test, probe_fails_on_file_shorter_than_locator) {
    std::string e0 = entry("k00");
    std::string e1 = normal_entry(1, "k01", std::string(10000, 'v'), 7, 1);
    const std::uint64_t claimed_size = e0.size() + e1.size() + e0.size();
    // e1 が 4 KiB 超の途中で切れている
    boost::filesystem::path p = write_file("snapshot", e0 + e1.substr(0, 5000));
    log_entry_reader reader(p);
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), claimed_size); }, "is cut by the file size");
    // e1 がヘッダの途中で切れている
    p = write_file("snapshot2", e0 + e1.substr(0, 5));
    log_entry_reader reader2(p);
    expect_throws([&] { (void) region_allocator::probe(reader2, e0.size(), claimed_size); }, "is cut by the file size");
    // 位置の先にデータがない
    p = write_file("snapshot3", e0);
    log_entry_reader reader3(p);
    expect_throws([&] { (void) region_allocator::probe(reader3, e0.size(), claimed_size); }, "is shorter than the locator says");
    // エントリの必要な分がファイルの大きさを超える
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), e0.size() + 5000); }, "is cut by the file size");
}

// 誤った使い方は abort する: R = 0、コンパクション済み WAL ファイルのロケータがあるのに reader がない
TEST_F(region_allocator_test, invalid_use_aborts) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01"}), 1);
    locator compacted = make_file("compacted", entries({"k00"}), 1);
    EXPECT_DEATH({ region_allocator a(snapshot, std::nullopt, 0); }, "region records is 0");
    region_allocator allocator(snapshot, compacted, 1);
    log_entry_reader snapshot_reader(path("snapshot"));
    EXPECT_DEATH((void) allocator.allocate(snapshot_reader, nullptr), "no reader for the compacted file");
}

}  // namespace limestone::testing
