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

// ローテーションの結果を表すクラス
class rotation_result {
public:
    // デフォルトコンストラクタ
    rotation_result();

    // ファイル名とepoch_idを引数に取るコンストラクタ
    rotation_result(std::string file, epoch_id_type epoch);

    [[nodiscard]] const std::set<std::string>& get_latest_rotated_files() const;
    [[nodiscard]] const std::set<std::string>& get_remaining_rotated_files() const;
    [[nodiscard]] std::optional<epoch_id_type> get_epoch_id() const;

    // 他のrotation_resultを追加するメソッド
    void add_rotation_result(const rotation_result& other);

    // この情報は別管理にすべきなので、おそらく不要になる
    void add_rotated_file(const std::string& file);

private:
    std::set<std::string> latest_rotated_files_;
    // この情報は別管理にすべきなので、おそらく不要になる
    std::set<std::string> remaining_rotated_files_;
    std::optional<epoch_id_type> epoch_id_;
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
    explicit rotation_task(datastore& envelope);
    
    datastore& envelope_;

    std::promise<rotation_result> result_promise_;
    std::future<rotation_result> result_future_;

    // `rotation_task_helper` が `rotation_task` を生成できるようにフレンドクラスに指定
    friend class rotation_task_helper;
};

// rotation_task_helperクラスの宣言
class rotation_task_helper {
public:
    static void enqueue_task(const std::shared_ptr<rotation_task>& task);
    static void attempt_task_execution_from_queue();
    static void clear_tasks(); // for testing
    static size_t queue_size(); // for testing


    // 新しいタスクを生成してキューに追加するヘルパーメソッド
    static std::shared_ptr<rotation_task> create_and_enqueue_task(datastore& envelope) {
        auto task = std::shared_ptr<rotation_task>(new rotation_task(envelope));
        enqueue_task(task);
        return task;
    }

private:
    static std::queue<std::shared_ptr<rotation_task>>& get_tasks();
    static std::mutex& get_mutex();
};

} // namespace limestone::api

#endif // ROTATION_TASK_H
