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

// 静的メンバ変数の初期化
std::queue<std::shared_ptr<rotation_task>> rotation_task_helper::tasks_;
std::mutex rotation_task_helper::mutex_;

// rotation_task_helperクラスの実装
void rotation_task_helper::enqueue_task(std::shared_ptr<rotation_task> task) {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(task);
}

void rotation_task_helper::execute_task() {
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

void rotation_task_helper::clear_tasks() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::queue<std::shared_ptr<rotation_task>> empty;
    std::swap(tasks_, empty);
}

} // namespace limestone::api
