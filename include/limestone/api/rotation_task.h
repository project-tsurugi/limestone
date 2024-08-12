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

class rotation_result {
public:
    rotation_result();

    rotation_result(std::string file, epoch_id_type epoch);

    [[nodiscard]] const std::set<std::string>& get_latest_rotated_files() const;
    [[nodiscard]] std::optional<epoch_id_type> get_epoch_id() const;
    [[nodiscard]] const std::set<boost::filesystem::path>& get_rotation_end_files() const;
    void set_rotation_end_files(const std::set<boost::filesystem::path>& files);

    void add_rotation_result(const rotation_result& other);
private:
    // A set of filenames that were rotated in this rotation process.
    std::set<std::string> latest_rotated_files_;

    // A set of file paths managed by the datastore at the end of this rotation.
    std::set<boost::filesystem::path> rotation_end_files;

    // The epoch ID at the time of the rotation. Any WAL entries with an epoch ID
    // equal to or greater than this are guaranteed not to be present in the rotated files.
    std::optional<epoch_id_type> epoch_id_;
};


// rotation_taskクラスの宣言
class rotation_task {
public:
    void rotate();

    rotation_result wait_for_result();

private:
    explicit rotation_task(datastore& envelope);
    
    datastore& envelope_;

    std::promise<rotation_result> result_promise_;
    std::future<rotation_result> result_future_;

    friend class rotation_task_helper;
};

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
