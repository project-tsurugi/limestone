#include "rotation_task.h"
#include <limestone/api/datastore.h>

namespace limestone::internal {
    using namespace limestone::api;


rotation_task::rotation_task(datastore& envelope) 
    : envelope_(envelope),  result_future_(result_promise_.get_future()) {}


void rotation_task::rotate() {
    // ダミーのローテーション処理
    rotation_result result;
    result.rotated_files = {"file1.log", "file2.log"};
    result.epoch_id = 123;

    // 結果をセット
    result_promise_.set_value(result);
}

rotation_result rotation_task::wait_for_result() {
    return result_future_.get();
}

// rotation_task_helper クラスの実装
std::queue<std::shared_ptr<rotation_task>> rotation_task_helper::tasks_;
std::mutex rotation_task_helper::mutex_;

void rotation_task_helper::enqueue_task(std::shared_ptr<rotation_task> task) {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(task);
}

void rotation_task_helper::attempt_task_execution_from_queue() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!tasks_.empty()) {
        auto task = tasks_.front();
        tasks_.pop();
        task->rotate();
    }
}

void rotation_task_helper::clear_tasks() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::queue<std::shared_ptr<rotation_task>> empty;
    std::swap(tasks_, empty);
}

} // namespace limestone::internal
