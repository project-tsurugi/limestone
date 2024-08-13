/*
 * Copyright 2022-2023 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <thread>
#include <chrono>
#include <iomanip>
#include <stdexcept>
#include <future>

#include <boost/filesystem/fstream.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "internal.h"
#include "compaction_catalog.h"
#include <limestone/api/rotation_task.h>
#include "log_entry.h"

namespace limestone::api {
using namespace limestone::internal;

datastore::datastore() noexcept = default;

datastore::datastore(configuration const& conf) : location_(conf.data_locations_.at(0)) {
    LOG(INFO) << "/:limestone:config:datastore setting log location = " << location_.string();
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(location_, error);
    boost::filesystem::path manifest_path = boost::filesystem::path(location_) / std::string(internal::manifest_file_name);
    boost::filesystem::path compaction_catalog_path= boost::filesystem::path(location_) / std::string(compaction_catalog::get_catalog_filename());
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(location_, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory: result_mkdir: " << result_mkdir << ", error_code: " << error << ", path: " << location_;
            throw std::runtime_error("fail to create the log_location directory");
        }
        internal::setup_initial_logdir(location_);
        add_file(manifest_path);
    } else {
        int count = 0;
        // use existing log-dir
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(location_)) {
            if (!boost::filesystem::is_directory(p)) {
                count++;
                add_file(p);
            }
        }
        if (count == 0) {
            internal::setup_initial_logdir(location_);
            add_file(manifest_path);
        }
    }
    internal::check_and_migrate_logdir_format(location_);
    add_file(compaction_catalog_path);

    // XXX: prusik era
    // TODO: read rotated epoch files if main epoch file does not exist
    epoch_file_path_ = location_ / boost::filesystem::path(std::string(epoch_file_name));
    const bool result = boost::filesystem::exists(epoch_file_path_, error);
    if (!result || error) {
        FILE* strm = fopen(epoch_file_path_.c_str(), "a");  // NOLINT(*-owning-memory)
        if (!strm) {
            LOG_LP(ERROR) << "does not have write permission for the log_location directory, path: " <<  location_;
            throw std::runtime_error("does not have write permission for the log_location directory");
        }
        if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
            LOG_LP(ERROR) << "fclose failed, errno = " << errno;
            throw std::runtime_error("I/O error");
        }
        add_file(epoch_file_path_);
    }

    recover_max_parallelism_ = conf.recover_max_parallelism_;
    LOG(INFO) << "/:limestone:config:datastore setting the number of recover process thread = " << recover_max_parallelism_;

    VLOG_LP(log_debug) << "datastore is created, location = " << location_.string();
}

datastore::~datastore() {
    if (online_compaction_worker_future_.valid()) {
        online_compaction_worker_future_.wait();
    }
}


void datastore::recover() const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

void datastore::ready() {
    create_snapshot();
    online_compaction_worker_future_ = std::async(std::launch::async, &datastore::online_compaction_worker, this);
    state_ = state::ready;
}

std::unique_ptr<snapshot> datastore::get_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::unique_ptr<snapshot>(new snapshot(location_));
}

std::shared_ptr<snapshot> datastore::shared_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::shared_ptr<snapshot>(new snapshot(location_));
}

log_channel& datastore::create_channel(const boost::filesystem::path& location) {
    check_before_ready(static_cast<const char*>(__func__));
    
    std::lock_guard<std::mutex> lock(mtx_channel_);
    
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(std::unique_ptr<log_channel>(new log_channel(location, id, *this)));  // constructor of log_channel is private
    return *log_channels_.at(id);
}

epoch_id_type datastore::last_epoch() const noexcept { return static_cast<epoch_id_type>(epoch_id_informed_.load()); }

void datastore::switch_epoch(epoch_id_type new_epoch_id) {
    check_after_ready(static_cast<const char*>(__func__));
    rotation_task_helper::attempt_task_execution_from_queue(); 
    auto neid = static_cast<std::uint64_t>(new_epoch_id);
    if (auto switched = epoch_id_switched_.load(); neid <= switched) {
        LOG_LP(WARNING) << "switch to epoch_id_type of " << neid << " (<=" << switched << ") is curious";
    }

    epoch_id_switched_.store(neid);
    update_min_epoch_id(true);
}

void datastore::update_min_epoch_id(bool from_switch_epoch) {  // NOLINT(readability-function-cognitive-complexity)
    auto upper_limit = epoch_id_switched_.load() - 1;
    epoch_id_type max_finished_epoch = 0;

    for (const auto& e : log_channels_) {
        auto working_epoch = static_cast<epoch_id_type>(e->current_epoch_id_.load());
        if ((working_epoch - 1) < upper_limit) {
            upper_limit = working_epoch - 1;
        }
        auto finished_epoch = e->finished_epoch_id_.load();
        if (max_finished_epoch < finished_epoch) {
            max_finished_epoch = finished_epoch;
        }
    }

    // update recorded_epoch_
    auto to_be_epoch = upper_limit;
    if (from_switch_epoch && (to_be_epoch > static_cast<std::uint64_t>(max_finished_epoch))) {
        to_be_epoch = static_cast<std::uint64_t>(max_finished_epoch);
    }
    auto old_epoch_id = epoch_id_recorded_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            std::lock_guard<std::mutex> lock(mtx_epoch_file_);

            FILE* strm = fopen(epoch_file_path_.c_str(), "a");  // NOLINT(*-owning-memory)
            if (!strm) {
                LOG_LP(ERROR) << "fopen failed, errno = " << errno;
                throw std::runtime_error("I/O error");
            }
            log_entry::durable_epoch(strm, static_cast<epoch_id_type>(epoch_id_recorded_.load()));
            if (fflush(strm) != 0) {
                LOG_LP(ERROR) << "fflush failed, errno = " << errno;
                throw std::runtime_error("I/O error");
            }
            if (fsync(fileno(strm)) != 0) {
                LOG_LP(ERROR) << "fsync failed, errno = " << errno;
                throw std::runtime_error("I/O error");
            }
            if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
                LOG_LP(ERROR) << "fclose failed, errno = " << errno;
                throw std::runtime_error("I/O error");
            }
            break;
        }
    }

    // update informed_epoch_
    to_be_epoch = upper_limit;
    old_epoch_id = epoch_id_informed_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_informed_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            if (persistent_callback_) {
                persistent_callback_(to_be_epoch);
            }
            break;
        }
    }
}

void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) noexcept {
    check_before_ready(static_cast<const char*>(__func__));
    persistent_callback_ = std::move(callback);
}

void datastore::switch_safe_snapshot([[maybe_unused]] write_version_type write_version, [[maybe_unused]] bool inclusive) const noexcept {
    check_after_ready(static_cast<const char*>(__func__));
}

void datastore::add_snapshot_callback(std::function<void(write_version_type)> callback) noexcept {
    check_before_ready(static_cast<const char*>(__func__));
    snapshot_callback_ = std::move(callback);
}

std::future<void> datastore::shutdown() noexcept {
    VLOG_LP(log_info) << "start";
    state_ = state::shutdown;

    stop_online_compaction_worker_.store(true);
    if (!online_compaction_worker_future_.valid()) {
        VLOG(log_info) << "Compaction task is not running. Skipping task shutdown.";
    } else {
        VLOG(log_info) << "shutdown: waiting for compaction task to stop";
        online_compaction_worker_future_.wait();
        VLOG(log_info) << "Compaction task has been stopped.";
    }

    return std::async(std::launch::async, []{
        std::this_thread::sleep_for(std::chrono::microseconds(100000));
        VLOG(log_info) << "/:limestone:datastore:shutdown end";
    });
}

// old interface
backup& datastore::begin_backup() {
    auto tmp_files = get_files();
    backup_ = std::unique_ptr<backup>(new backup(tmp_files));
    return *backup_;
}

std::unique_ptr<backup_detail> datastore::begin_backup(backup_type btype) {  // NOLINT(readability-function-cognitive-complexity)
   rotation_result result = rotate_log_files();

    // LOG-0: all files are log file, so all files are selected in both standard/transaction mode.
    (void) btype;

    // calcuate files_ minus active-files
    std::set<boost::filesystem::path> inactive_files(result.get_rotation_end_files());
    inactive_files.erase(epoch_file_path_);
    for (const auto& lc : log_channels_) {
        if (lc->registered_) {
            inactive_files.erase(lc->file_path());
        }
    }

    // build entries
    std::vector<backup_detail::entry> entries;
    for (auto & ent : inactive_files) {
        // LOG-0: assume files are located flat in logdir.
        std::string filename = ent.filename().string();
        auto dst = filename;
        switch (filename[0]) {
            case 'p': {
                if (filename.find("wal", 1) == 1) {
                    // "pwal"
                    // pwal files are type:logfile, detached

                    // skip an "inactive" file with the name of active file,
                    // it will cause some trouble if a file (that has the name of mutable files) is saved as immutable file.
                    // but, by skip, backup files may be imcomplete.
                    if (filename.length() == 9) {  // FIXME: too adohoc check
                        boost::system::error_code error;
                        bool result = boost::filesystem::is_empty(ent, error);
                        if (!error && !result) {
                            LOG_LP(ERROR) << "skip the file with the name like active files: " << filename;
                        }
                        continue;
                    }
                    entries.emplace_back(ent.string(), dst, false, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'e': {
                if (filename.find("poch", 1) == 1) {
                    // "epoch"
                    // epoch file(s) are type:logfile, the last rotated file is non-detached

                    // skip active file
                    if (filename.length() == 5) {  // FIXME: too adohoc check
                        continue;
                    }

                    // TODO: only last epoch file is not-detached
                    entries.emplace_back(ent.string(), dst, false, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'l': {
                if (filename == internal::manifest_file_name) {
                    entries.emplace_back(ent.string(), dst, true, false);
                } else {
                    // unknown type
                }
                break;
            }
            case 'c': {
                if (filename == compaction_catalog::get_catalog_filename()) {
                    entries.emplace_back(ent.string(), dst, false, false);
                } else if(filename.find("mpct", 1) == 1) {
                    entries.emplace_back(ent.string(), dst, false, false);
                }
                break;
            }
            default: {
                // unknown type
            }
        }
    }
    return std::unique_ptr<backup_detail>(new backup_detail(entries, epoch_id_switched_.load()));
}

tag_repository& datastore::epoch_tag_repository() noexcept {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] const epoch_tag& tag) const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

rotation_result datastore::rotate_log_files() {
    // Create and enqueue a rotation task.
    // Rotation task is executed when switch_epoch() is called.
    // Wait for the result of the rotation task.
    auto task = rotation_task_helper::create_and_enqueue_task(*this);
    rotation_result result = task->wait_for_result();

    // Wait for all log channels to complete the session with the specified session ID.
    for(auto& lc: log_channels_) {
        lc->wait_for_end_session(result.get_epoch_id().value());
    }
    return result;
}

void datastore::rotate_epoch_file() {
    // XXX: multi-thread broken

    std::stringstream ss;
    ss << "epoch."
       << std::setw(14) << std::setfill('0') << current_unix_epoch_in_millis()
       << "." << epoch_id_switched_.load();
    std::string new_name = ss.str();
    boost::filesystem::path new_file = location_ / new_name;
    boost::filesystem::rename(epoch_file_path_, new_file);
    add_file(new_file);

    // create new one
    boost::filesystem::ofstream strm{};
    strm.open(epoch_file_path_, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    if(!strm || !strm.is_open() || strm.bad() || strm.fail()){
        LOG_LP(ERROR) << "does not have write permission for the log_location directory, path: " <<  location_;
        throw std::runtime_error("does not have write permission for the log_location directory");
    }
    strm.close();
}

void datastore::add_file(const boost::filesystem::path& file) noexcept {
    std::lock_guard<std::mutex> lock(mtx_files_);

    files_.insert(file);
}

void datastore::subtract_file(const boost::filesystem::path& file) {
    std::lock_guard<std::mutex> lock(mtx_files_);

    files_.erase(file);
}

std::set<boost::filesystem::path> datastore::get_files() {
    std::lock_guard<std::mutex> lock(mtx_files_);

    return files_;
}

void datastore::check_after_ready(std::string_view func) const noexcept {
    if (state_ == state::not_ready) {
        LOG_LP(WARNING) << func << " called before ready()";
    }
}

void datastore::check_before_ready(std::string_view func) const noexcept {
    if (state_ != state::not_ready) {
        LOG_LP(WARNING) << func << " called after ready()";
    }
}

int64_t datastore::current_unix_epoch_in_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void datastore::online_compaction_worker() {
    std::cout << "Compaction task started..." << std::endl;
    while (!stop_online_compaction_worker_) {
        // 模擬的なコンパクション処理をここに記述
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "Compaction task running..." << std::endl;
    }
    std::cout << "Compaction task stopped." << std::endl;
}

} // namespace limestone::api
