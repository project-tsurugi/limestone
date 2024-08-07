#include <limestone/api/rotation_task.h>
#include <limestone/api/datastore.h>

namespace limestone::api {

// デフォルトコンストラクタ
rotation_result::rotation_result() = default;

// ファイル名とepoch_idを引数に取るコンストラクタ
rotation_result::rotation_result(std::vector<std::string> files, epoch_id_type epoch)
    : rotated_files_(std::move(files)), epoch_id_(epoch) {}

// Getter
const std::vector<std::string>& rotation_result::get_rotated_files() const {
    return rotated_files_;
}

std::optional<epoch_id_type> rotation_result::get_epoch_id() const {
    return epoch_id_;
}

// 他のrotation_resultを追加するメソッド
void rotation_result::add_rotation_result(const rotation_result& other) {
    // rotated_files_に追加
    rotated_files_.insert(rotated_files_.end(), other.rotated_files_.begin(), other.rotated_files_.end());

    // epoch_id_が未設定の場合、otherのepoch_id_を設定
    if (!epoch_id_.has_value()) {
        epoch_id_ = other.epoch_id_;
    } else if (other.epoch_id_.has_value() && epoch_id_ != other.epoch_id_) {
        // 既に設定されているepoch_id_と異なる場合は例外を投げる
        throw std::runtime_error("Conflicting epoch_id in rotation results");
    }
}


rotation_task::rotation_task(datastore& envelope) 
    : envelope_(envelope),  result_future_(result_promise_.get_future()) {}


void rotation_task::rotate() {
    rotation_result final_result;
    for (const auto& lc : envelope_.log_channels_) {
        boost::system::error_code error;
        bool result = boost::filesystem::exists(lc->file_path(), error);
        if (!result || error) {
            continue;  // skip if not exists
        }
        // The following code was commented out because it introduces a bug:
        // Checking if the file is empty and skipping it can cause valid log files 
        // to be ignored during rotation, leading to potential data loss or inconsistency.
        // The rotation process should handle all files that exist, regardless of their size.
        // 
        // result = boost::filesystem::is_empty(lc->file_path(), error);
        // if (result || error) {
        //     continue;  // skip if empty
        // }
        rotation_result channel_result = lc->do_rotate_file();
        final_result.add_rotation_result(channel_result);
    }
    envelope_.rotate_epoch_file();

    // 結果をセット
    result_promise_.set_value(final_result);
}

rotation_result rotation_task::wait_for_result() {
    return result_future_.get();
}

void rotation_task_helper::enqueue_task(const std::shared_ptr<rotation_task>& task) {
    std::lock_guard<std::mutex> lock(get_mutex());
    get_tasks().push(task);
}

void rotation_task_helper::attempt_task_execution_from_queue() {
    std::lock_guard<std::mutex> lock(get_mutex());
    if (!get_tasks().empty()) {
        auto task = get_tasks().front();
        get_tasks().pop();
        task->rotate();
    }
}

void rotation_task_helper::clear_tasks() {
    std::lock_guard<std::mutex> lock(get_mutex());
    std::queue<std::shared_ptr<rotation_task>> empty;
    std::swap(get_tasks(), empty);
}

std::queue<std::shared_ptr<rotation_task>>& rotation_task_helper::get_tasks() {
    static std::queue<std::shared_ptr<rotation_task>> tasks_;
    return tasks_;
}

std::mutex& rotation_task_helper::get_mutex() {
    static std::mutex mutex_;
    return mutex_;
}

} // namespace limestone::api
