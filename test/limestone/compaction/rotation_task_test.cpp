#include "rotation_task.h"
#include <gtest/gtest.h>

namespace limestone::api {

class rotation_task_test : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
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
    rotation_task_manager manager;

    auto task1 = std::make_shared<rotation_task>();
    manager.enqueue_task(task1);

    auto task2 = std::make_shared<rotation_task>();
    manager.enqueue_task(task2);

    manager.execute_next_task();
    rotation_result result1 = task1->get_result();
    EXPECT_EQ(result1.rotated_files.size(), 2);
    EXPECT_EQ(result1.epoch_id, 123);

    manager.execute_next_task();
    rotation_result result2 = task2->get_result();
    EXPECT_EQ(result2.rotated_files.size(), 2);
    EXPECT_EQ(result2.epoch_id, 123);
}

TEST_F(rotation_task_test, no_task_execution_when_queue_is_empty) {
    rotation_task_manager manager;

    manager.execute_next_task();

    SUCCEED();
}

}  // namespace limestone::api
