#include <limestone/api/rotation_task.h>
#include <gtest/gtest.h>
#include <limestone/api/datastore.h>
#include "test_root.h"

namespace limestone::api {

constexpr const char* data_location = "/tmp/rotation_task_test/data_location";
constexpr const char* metadata_location = "/tmp/rotation_task_test/metadata_location";

const boost::filesystem::path epoch_path{std::string(data_location) + "/epoch"};

class rotation_task_test : public ::testing::Test {
protected:
    void SetUp() override {
        // 既存のディレクトリやファイルを削除
        if (system("rm -rf /tmp/rotation_task_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        // 必要なディレクトリを作成
        if (system("mkdir -p /tmp/rotation_task_test/data_location /tmp/rotation_task_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        // pwal3は存在しない

        // データストアのセットアップ
        limestone::api::configuration conf({data_location}, metadata_location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->switch_epoch(123);
        boost::filesystem::path location_path(data_location);
        lc0_ = &datastore_->create_channel(location_path);
        lc1_ = &datastore_->create_channel(location_path);
        lc2_ = &datastore_->create_channel(location_path);
        lc3_ = &datastore_->create_channel(location_path);

        // Add entries to each log channel
        write_to_channel(lc0_);
        write_to_channel(lc1_);
        write_to_channel(lc2_);
        // No entries for lc3

    }
    void write_to_channel(log_channel* channel) {
        channel->begin_session();
        channel->add_entry(1, "k1", "v1", {4, 0});
        channel->end_session();
    }

    void TearDown() override {
        // テスト後にキューをクリアする
        rotation_task_helper::clear_tasks();
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    log_channel* lc0_{};
    log_channel* lc1_{};
    log_channel* lc2_{};
    log_channel* lc3_{};

    const std::string pwal0{"pwal_0000"};
    const std::string pwal1{"pwal_0001"};
    const std::string pwal2{"pwal_0002"};
    const std::string pwal3{"pwal_0003"};

};

// ファイル名の前方一致をチェックする関数
void check_rotated_file(const std::set<std::string>& actual_files, const std::string& expected_filename) {
    auto starts_with = [](const std::string& full_string, const std::string& prefix) {
        return full_string.find(prefix) == 0;
    };

    bool match_found = false;
    for (const auto& actual_file : actual_files) {
        std::string actual_filename = boost::filesystem::path(actual_file).filename().string();
        if (starts_with(actual_filename, expected_filename)) {
            match_found = true;
            break;
        }
    }

    EXPECT_TRUE(match_found)
        << "Expected filename to start with: " << expected_filename << ", but none of the actual files matched.";
}

TEST_F(rotation_task_test, rotate_sets_result) {
    auto task = rotation_task_helper::create_and_enqueue_task(*datastore_);

    task->rotate();
    rotation_result result = task->wait_for_result();
    EXPECT_EQ(result.get_latest_rotated_files().size(), 3);
    check_rotated_file(result.get_latest_rotated_files(), pwal0);
    check_rotated_file(result.get_latest_rotated_files(), pwal1);
    check_rotated_file(result.get_latest_rotated_files(), pwal2);
    EXPECT_EQ(result.get_epoch_id(), 123);
}

TEST_F(rotation_task_test, enqueue_and_execute_task) {
    EXPECT_EQ(rotation_task_helper::queue_size(), 0);
    auto task1 = rotation_task_helper::create_and_enqueue_task(*datastore_);
    EXPECT_EQ(rotation_task_helper::queue_size(), 1);
    auto task2 = rotation_task_helper::create_and_enqueue_task(*datastore_);
    EXPECT_EQ(rotation_task_helper::queue_size(), 2);

    datastore_->switch_epoch(124); // dexecute rotation_task in switch_epoch
    EXPECT_EQ(rotation_task_helper::queue_size(), 1);
    rotation_result result1 = task1->wait_for_result();
    EXPECT_EQ(result1.get_latest_rotated_files().size(), 3);
    check_rotated_file(result1.get_latest_rotated_files(), pwal0);
    check_rotated_file(result1.get_latest_rotated_files(), pwal1);
    check_rotated_file(result1.get_latest_rotated_files(), pwal2);
    EXPECT_EQ(result1.get_epoch_id(), 123);

    write_to_channel(lc3_);
    EXPECT_EQ(rotation_task_helper::queue_size(), 1);
    datastore_->switch_epoch(125); // dexecute rotation_task in switch_epoch
    EXPECT_EQ(rotation_task_helper::queue_size(), 0);
    rotation_result result2 = task2->wait_for_result();
    EXPECT_EQ(result2.get_latest_rotated_files().size(), 1);
    check_rotated_file(result2.get_latest_rotated_files(), pwal3);
    EXPECT_EQ(result2.get_epoch_id(), 124);
}

TEST_F(rotation_task_test, no_task_execution_when_queue_is_empty) {
    rotation_task_helper::attempt_task_execution_from_queue();

    SUCCEED();
}

}  // namespace limestone::api
