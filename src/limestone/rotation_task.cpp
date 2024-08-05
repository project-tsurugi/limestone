#include <limestone/api/rotation_task.h>
#include <limestone/api/datastore.h>

namespace limestone::api {

rotation_task::rotation_task(datastore& envelope) 
    : envelope_(envelope),  result_future_(result_promise_.get_future()) {}


void rotation_task::rotate() {
    envelope_.add_file(log_file);   

    // TODO:
    //   for each logchannel lc:
    //     if lc is in session, reserve do_rotate for end-of-session
    //               otherwise, lc.do_rotate_file() immediately
    //   rotate epoch file

    // XXX: adhoc implementation:
    //   for each logchannel lc:
    //       lc.do_rotate_file()
    //   rotate epoch file
    for (const auto& lc : log_channels_) {
#if 0
        // XXX: this condition may miss log-files made before this process and not rotated
        if (!lc->registered_) {
            continue;
        }
#else
        boost::system::error_code error;
        bool result = boost::filesystem::exists(lc->file_path(), error);
        if (!result || error) {
            continue;  // skip if not exists
        }
        result = boost::filesystem::is_empty(lc->file_path(), error);
        if (result || error) {
            continue;  // skip if empty
        }
#endif
        lc->do_rotate_file();
    }
    envelope_.rotate_epoch_file();

        // ダミーのローテーション処理
    rotation_result result;
    result.rotated_files = {"file1.log", "file2.log"};
    result.epoch_id = envelope_.epoch_id_switched_.load();

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

} // namespace limestone::api
