#include "rotation_task.h"
#include <thread>
#include <chrono>

namespace limestone::api {

// rotation_taskクラスの実装
rotation_task::rotation_task() : result_promise_(), result_future_(result_promise_.get_future()) {}

void rotation_task::rotate() {
    // ローテーション処理のシミュレーション（実際のローテーション処理に置き換えてください）
    std::this_thread::sleep_for(std::chrono::seconds(2)); // 例: 2秒待つ
    
    // ローテーション結果を作成
    rotation_result result;
    result.rotated_files = {"file1.log", "file2.log"}; // 例としていくつかのファイル名
    result.epoch_id = 123; // 例としてエポックIDを設定

    // 結果を設定
    result_promise_.set_value(result);
}

rotation_result rotation_task::get_result() {
    return result_future_.get(); // 結果が得られるまで待機し、結果を返す
}

// rotation_task_managerクラスの実装
void rotation_task_manager::enqueue_task(std::shared_ptr<rotation_task> task) {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(task);
}

void rotation_task_manager::execute_next_task() {
    std::shared_ptr<rotation_task> task;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tasks_.empty()) {
            return; // キューが空の場合は何もしない
        }

        task = tasks_.front();
        tasks_.pop();
    }

    // タスクを実行
    task->rotate();
}

bool rotation_task_manager::has_pending_tasks() {
    std::lock_guard<std::mutex> lock(mutex_);
    return !tasks_.empty();
}

} // namespace limestone::api