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
#include <cstdio>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/snapshot.h>

#include "limestone/compaction/compaction_test_fixture.h"
#include "datastore_impl.h"
#include "locator/locator_format.h"

namespace limestone::testing {

/**
 * get_partitioned_cursors() の総括テスト (datastore の層)。datastore に書いたエントリから実際に作られたスナップショット
 * ファイルとコンパクション済み WAL ファイル (とそれらのロケータファイル) を、公開 API の get_partitioned_cursors() で読み、
 * get_cursor() の出力と一致することを確かめる。
 */
class partitioned_reading_datastore_test : public compaction_test {
public:
    partitioned_reading_datastore_test() : compaction_test("/tmp/partitioned_reading_datastore_test") {}

    /// cursor が返す 1 件 (storage、key、value)
    using row = std::tuple<std::uint64_t, std::string, std::string>;
    using rows = std::vector<row>;

    // 記録間隔を小さくして (64 バイト、R = 1) datastore を起動する。ready() でスナップショットファイルとそのロケータ
    // ファイルが作られる
    void start_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_locator_settings_for_test({64, 1});
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
        datastore_->wait_for_blob_file_garbace_collector();
    }

    void restart_datastore() {
        datastore_->shutdown();
        datastore_ = nullptr;
        start_datastore();
    }

    static std::string key_of(int i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%04d", i);
        return buf;
    }

    // 1 つのセッションで keys のエントリを書き、エポックを進める
    void write_entries(int first, int last, const std::string& value, epoch_id_type epoch) {
        lc0_->begin_session();
        for (int i = first; i < last; ++i) {
            lc0_->add_entry(1, key_of(i), value + key_of(i), {epoch, 0});
        }
        lc0_->end_session();
        datastore_->switch_epoch(epoch + 1);
    }

    void remove_entries(int first, int last, int step, epoch_id_type epoch) {
        lc0_->begin_session();
        for (int i = first; i < last; i += step) {
            lc0_->remove_entry(1, key_of(i), {epoch, 0});
        }
        lc0_->end_session();
        datastore_->switch_epoch(epoch + 1);
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

    // get_cursor() の出力と、n 本の cursor の出力を合わせたものを比べる。各 cursor の出力は昇順であること
    void check_partitioned_matches_full(const std::vector<std::size_t>& ns, std::size_t expected_count) {
        const rows expected = read_all(*datastore_->get_snapshot()->get_cursor());
        ASSERT_EQ(expected.size(), expected_count);
        ASSERT_TRUE(std::is_sorted(expected.begin(), expected.end()));
        for (std::size_t n : ns) {
            std::unique_ptr<api::snapshot> snapshot = datastore_->get_snapshot();
            std::vector<std::unique_ptr<api::cursor>> cursors = snapshot->get_partitioned_cursors(n);
            ASSERT_EQ(cursors.size(), n);
            rows all{};
            for (std::unique_ptr<api::cursor>& cursor : cursors) {
                rows r = read_all(*cursor);
                EXPECT_TRUE(std::is_sorted(r.begin(), r.end())) << "n = " << n;
                all.insert(all.end(), r.begin(), r.end());
            }
            std::sort(all.begin(), all.end());
            EXPECT_EQ(all, expected) << "n = " << n;
        }
    }
};

// コンパクション済み WAL ファイルがなく、スナップショットファイルだけの場合
TEST_F(partitioned_reading_datastore_test, snapshot_only) {
    start_datastore();
    datastore_->switch_epoch(1);
    write_entries(0, 300, "a", 1);
    write_entries(100, 200, "b", 2);  // 上書き
    remove_entries(0, 300, 7, 3);
    restart_datastore();
    EXPECT_TRUE(compacted_filename().empty());
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "data" / "snapshot.loc"));
    // 300 件のうち 7 の倍数 (43 件) が削除されている
    check_partitioned_matches_full({1, 2, 3, 8}, 300 - 43);
}

// コンパクション済み WAL ファイルがある場合: コンパクションの後に書いたエントリがスナップショットファイルに、前に書いた
// エントリがコンパクション済み WAL ファイルにある。同じキーの上書きと削除を含む
TEST_F(partitioned_reading_datastore_test, with_compacted_file) {
    start_datastore();
    datastore_->switch_epoch(1);
    write_entries(0, 400, "a", 1);
    write_entries(50, 150, "b", 2);
    run_compact_with_epoch_switch(4);
    ASSERT_FALSE(compacted_filename().empty());
    write_entries(300, 500, "c", 5);  // 100 件の上書きと 100 件の追加
    remove_entries(0, 100, 5, 6);      // コンパクション済み側の 20 件を削除
    restart_datastore();
    ASSERT_FALSE(compacted_filename().empty());
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location)
                                          / internal::locator_format::locator_file_name(compacted_filename())));
    check_partitioned_matches_full({1, 2, 3, 8}, 500 - 20);
}

}  // namespace limestone::testing
