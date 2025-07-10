#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "limestone/api/log_channel.h"
#include "snapshot_impl.h"
#include "cursor_impl.h"
#include "test_root.h"

namespace limestone::testing {

using limestone::api::log_channel;

class snapshot_impl_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/snapshot_impl_test";
    std::unique_ptr<limestone::api::datastore_test> datastore_;
    log_channel* lc0_{};

    void SetUp() override {
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directory(location);
        gen_datastore();
    }

    void TearDown() override {
        datastore_ = nullptr;
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{ location };
        limestone::api::configuration conf(data_locations, location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        lc0_ = &datastore_->create_channel(location);
        datastore_->ready();
    }

    void create_log_file(const std::string& filename,
                         const std::vector<std::tuple<api::storage_id_type, std::string, std::string, api::write_version_type>>& entries) {
        lc0_->begin_session();
        for (const auto& entry : entries) {
            lc0_->add_entry(std::get<0>(entry), std::get<1>(entry), std::get<2>(entry), std::get<3>(entry));
        }
        lc0_->end_session();

        // 元のファイル（ログ出力）
        boost::filesystem::path pwal_file = boost::filesystem::path(location) / "pwal_0000";

        // target_file を location からの相対パスで構築
        boost::filesystem::path target_file = boost::filesystem::path(location) / filename;

        // 親ディレクトリを作成
        boost::filesystem::create_directories(target_file.parent_path());

        // ファイル移動
        if (boost::filesystem::exists(pwal_file)) {
            boost::filesystem::rename(pwal_file, target_file);
        } else {
            throw std::runtime_error("expected pwal_0000 file not found");
        }
    }
};

TEST_F(snapshot_impl_test, get_partitioned_cursors_returns_all_entries) {
    // 準備：2件のエントリを含む snapshot ファイルを作成
    std::vector<std::tuple<api::storage_id_type, std::string, std::string, api::write_version_type>> entries = {
        {1, "key1", "value1", {1, 0}},
        {1, "key2", "value2", {1, 1}},
    };
    create_log_file("data/snapshot", entries);

    // テスト対象：snapshot_impl 経由で partitioned_cursors を取得
    limestone::internal::snapshot_impl snapshot(location, {});
    auto cursors = snapshot.get_partitioned_cursors(2);

    // 各カーソルから全エントリを収集
    std::vector<std::pair<std::string, std::string>> actual;
    for (auto& cursor : cursors) {
        while (cursor->next()) {
            std::string key, value;
            cursor->key(key);
            cursor->value(value);
            actual.emplace_back(key, value);
        }
    }

    // 入力データと同等の内容が得られているか検証（順序は問わない）
    std::set<std::pair<std::string, std::string>> expected_set = {
        {"key1", "value1"},
        {"key2", "value2"},
    };
    std::set<std::pair<std::string, std::string>> actual_set(actual.begin(), actual.end());
    EXPECT_EQ(actual_set, expected_set);
}

TEST_F(snapshot_impl_test, get_partitioned_cursors_throws_on_zero_partition) {
    limestone::internal::snapshot_impl snapshot(location, {});
    EXPECT_THROW(snapshot.get_partitioned_cursors(0), std::invalid_argument);
}

TEST_F(snapshot_impl_test, get_partitioned_cursors_throws_on_second_call) {
    // 有効な log entry を含む snapshot を作成
    std::vector<std::tuple<api::storage_id_type, std::string, std::string, api::write_version_type>> entries = {
        {1, "key", "value", {1, 0}},
    };
    create_log_file("data/snapshot", entries);

    limestone::internal::snapshot_impl snapshot(location, {});
    auto cursors = snapshot.get_partitioned_cursors(1);

    // 2回目の呼び出しで例外が投げられることを確認
    EXPECT_THROW(snapshot.get_partitioned_cursors(1), limestone::api::limestone_exception);
}


TEST_F(snapshot_impl_test, get_partitioned_cursors_reads_compacted_file_if_exists) {
    // snapshot 側のエントリ
    create_log_file("data/snapshot", {
        {1, "key2", "value2", {1, 1}},
    });

    // compacted 側のエントリ
    create_log_file("pwal_0000.compacted", {
        {1, "key1", "value1", {1, 0}},
    });

    EXPECT_TRUE(boost::filesystem::exists(location + std::string("/data/snapshot")));
    EXPECT_TRUE(boost::filesystem::exists(location + std::string("/pwal_0000.compacted")));

    limestone::internal::snapshot_impl snapshot(location, {});
    auto cursors = snapshot.get_partitioned_cursors(2);

    std::vector<std::pair<std::string, std::string>> actual;
    for (auto& cursor : cursors) {
        while (cursor->next()) {
            std::string key, value;
            cursor->key(key);
            cursor->value(value);
            actual.emplace_back(key, value);
        }
    }

    std::set<std::pair<std::string, std::string>> expected_set = {
        {"key1", "value1"},
        {"key2", "value2"},
    };
    std::set<std::pair<std::string, std::string>> actual_set(actual.begin(), actual.end());
    EXPECT_EQ(actual_set, expected_set);
}

TEST_F(snapshot_impl_test, get_cursor_returns_entries_from_snapshot_only) {
    create_log_file("data/snapshot", {
        {1, "key1", "value1", {1, 1}},
        {1, "key2", "value2", {1, 2}},
    });

    limestone::internal::snapshot_impl snapshot(location, {});
    auto cursor = snapshot.get_cursor();

    std::vector<std::pair<std::string, std::string>> actual;
    while (cursor->next()) {
        std::string key, value;
        cursor->key(key);
        cursor->value(value);
        actual.emplace_back(key, value);
    }

    std::set<std::pair<std::string, std::string>> expected = {
        {"key1", "value1"},
        {"key2", "value2"},
    };
    EXPECT_EQ(std::set(actual.begin(), actual.end()), expected);
}

TEST_F(snapshot_impl_test, get_cursor_reads_compacted_if_exists) {
    // snapshot 側
    create_log_file("data/snapshot", {
        {1, "key2", "value2", {1, 1}},
    });

    // compacted 側
    create_log_file("pwal_0000.compacted", {
        {1, "key1", "value1", {1, 0}},
    });

    limestone::internal::snapshot_impl snapshot(location, {});
    auto cursor = snapshot.get_cursor();

    std::vector<std::pair<std::string, std::string>> actual;
    while (cursor->next()) {
        std::string key, value;
        cursor->key(key);
        cursor->value(value);
        actual.emplace_back(key, value);
    }

    std::set<std::pair<std::string, std::string>> expected = {
        {"key1", "value1"},
        {"key2", "value2"},
    };
    EXPECT_EQ(std::set(actual.begin(), actual.end()), expected);
}

} // namespace limestone::testing
