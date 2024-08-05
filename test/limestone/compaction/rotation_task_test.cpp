#include "rotation_task.h"
#include <gtest/gtest.h>

namespace limestone::api {

class rotation_task_test : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {
        // テスト後にキューをクリアする
        rotation_task_helper::clear_tasks();
    }
};

TEST_F(rotation_task_test, rotate_sets_result) {
    rotation_task task;

    std::thread t([&task]() {
        task.rotate();
    });

    t.join();

    rotation_result result = task.get_result();
    EXPECT_EQ(result.rotated_files.size(), 2);
    EXPECT_EQ(result.rotated_files[0], "file1.log");
    EXPECT_EQ(result.rotated_files[1], "file2.log");
    EXPECT_EQ(result.epoch_id, 123);
}

TEST_F(rotation_task_test, enqueue_and_execute_task) {
    auto task1 = std::make_shared<rotation_task>();
    rotation_task_helper::enqueue_task(task1);

    auto task2 = std::make_shared<rotation_task>();
    rotation_task_helper::enqueue_task(task2);

    rotation_task_helper::execute_task();
    rotation_result result1 = task1->get_result();
    EXPECT_EQ(result1.rotated_files.size(), 2);
    EXPECT_EQ(result1.epoch_id, 123);

    rotation_task_helper::execute_task();
    rotation_result result2 = task2->get_result();
    EXPECT_EQ(result2.rotated_files.size(), 2);
    EXPECT_EQ(result2.epoch_id, 123);
}

TEST_F(rotation_task_test, no_task_execution_when_queue_is_empty) {
    rotation_task_helper::execute_task();

    SUCCEED();
}

}  // namespace limestone::api
