#ifndef ROTATION_TASK_H
#define ROTATION_TASK_H

#include <iostream>
#include <future>
#include <vector>
#include <string>
#include <queue>
#include <mutex>
#include <memory>

#include "limestone/api/epoch_id_type.h"

namespace limestone::api {

// ローテーションの結果を表す構造体
struct rotation_result {
    std::vector<std::string> rotated_files;
    epoch_id_type epoch_id;
};

// rotation_taskクラスの宣言
class rotation_task {
public:
    rotation_task();

    // ローテーションを行うメソッド
    void rotate();

    // ローテーション結果を取得するメソッド
    rotation_result get_result();

private:
    std::promise<rotation_result> result_promise_;
    std::future<rotation_result> result_future_;
};

// rotation_task_helperクラスの宣言
class rotation_task_helper {
public:
    static void enqueue_task(std::shared_ptr<rotation_task> task);
    static void execute_task();
    static void clear_tasks(); // 追加: テストのためにキューをクリアするメソッド

private:
    static std::queue<std::shared_ptr<rotation_task>> tasks_;
    static std::mutex mutex_;
};

} // namespace limestone::api

#endif // ROTATION_TASK_H
