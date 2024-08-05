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
#include "limestone/api/datastore.h"
namespace limestone::api {

// ローテーションの結果を表す構造体
struct rotation_result {
    std::vector<std::string> rotated_files;
    epoch_id_type epoch_id;
};

// rotation_taskクラスの宣言
class rotation_task {
public:
    // ローテーションを行うメソッド
    void rotate();

    // ローテーション結果を取得するメソッド
    rotation_result wait_for_result();

private:
    // コンストラクタをprivateにして直接インスタンス化できないようにする
    rotation_task(datastore& envelope);
    
    datastore& envelope_;

    std::promise<rotation_result> result_promise_;
    std::future<rotation_result> result_future_;

    // `rotation_task_helper` が `rotation_task` を生成できるようにフレンドクラスに指定
    friend class rotation_task_helper;
};

// rotation_task_helperクラスの宣言
class rotation_task_helper {
public:
    static void enqueue_task(std::shared_ptr<rotation_task> task);
    static void attempt_task_execution_from_queue();
    static void clear_tasks(); // 追加: テストのためにキューをクリアするメソッド

    // 新しいタスクを生成してキューに追加するヘルパーメソッド
    static std::shared_ptr<rotation_task> create_and_enqueue_task(datastore& envelope) {
        auto task = std::shared_ptr<rotation_task>(new rotation_task(envelope));
        enqueue_task(task);
        return task;
    }

private:
    static std::queue<std::shared_ptr<rotation_task>> tasks_;
    static std::mutex mutex_;
};

} // namespace limestone::api

#endif // ROTATION_TASK_H
