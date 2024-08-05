#include <limestone/api/rotation_task.h>
#include <gtest/gtest.h>
#include "test_root.h"

namespace limestone::api {

constexpr const char* data_location = "/tmp/rotation_task_test/data_location";
constexpr const char* metadata_location = "/tmp/rotation_task_test/metadata_location";
class rotation_task_test : public ::testing::Test {
protected:
    void SetUp() override {
        if (system("rm -rf /tmp/rotation_task_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/rotation_task_test/data_location /tmp/rotation_task_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() override {
        // テスト後にキューをクリアする
        rotation_task_helper::clear_tasks();
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(rotation_task_test, rotate_sets_result) {
    auto task = rotation_task_helper::create_and_enqueue_task(*datastore_);

    std::thread t([&task]() {
        task->rotate();
    });

    t.join();

    rotation_result result = task->wait_for_result();
    EXPECT_EQ(result.rotated_files.size(), 2);
    EXPECT_EQ(result.rotated_files[0], "file1.log");
    EXPECT_EQ(result.rotated_files[1], "file2.log");
    EXPECT_EQ(result.epoch_id, 123);
}

TEST_F(rotation_task_test, enqueue_and_execute_task) {
    auto task1 = rotation_task_helper::create_and_enqueue_task(*datastore_);
    auto task2 = rotation_task_helper::create_and_enqueue_task(*datastore_);

    rotation_task_helper::attempt_task_execution_from_queue();
    rotation_result result1 = task1->wait_for_result();
    EXPECT_EQ(result1.rotated_files.size(), 2);
    EXPECT_EQ(result1.epoch_id, 123);

    rotation_task_helper::attempt_task_execution_from_queue();
    rotation_result result2 = task2->wait_for_result();
    EXPECT_EQ(result2.rotated_files.size(), 2);
    EXPECT_EQ(result2.epoch_id, 123);
}

TEST_F(rotation_task_test, no_task_execution_when_queue_is_empty) {
    rotation_task_helper::attempt_task_execution_from_queue();

    SUCCEED();
}

}  // namespace limestone::api
