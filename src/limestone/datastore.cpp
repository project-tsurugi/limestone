/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include "limestone_exception_helper.h"

#include <limestone/api/datastore.h>
#include "internal.h"
#include "rotation_result.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"

namespace limestone::api {
using namespace limestone::internal;

datastore::datastore() noexcept = default;

datastore::datastore(configuration const& conf) : location_(conf.data_locations_.at(0)) { // NOLINT(readability-function-cognitive-complexity)
    try {
        LOG(INFO) << "/:limestone:config:datastore setting log location = " << location_.string();
        boost::system::error_code error;
        const bool result_check = boost::filesystem::exists(location_, error);
        boost::filesystem::path manifest_path = boost::filesystem::path(location_) / std::string(internal::manifest_file_name);
        boost::filesystem::path compaction_catalog_path= boost::filesystem::path(location_) / compaction_catalog::get_catalog_filename();
        if (!result_check || error) {
            const bool result_mkdir = boost::filesystem::create_directory(location_, error);
            if (!result_mkdir || error) {
                LOG_AND_THROW_IO_EXCEPTION("fail to create directory: " + location_.string(), error);
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

        // acquire lock for manifest file
        fd_for_flock_ = internal::acquire_manifest_lock(location_);
        if (fd_for_flock_ == -1) {
            if (errno == EWOULDBLOCK) {
                std::string err_msg = "another process is using the log directory: " + location_.string();
                LOG(FATAL) << "/:limestone:config:datastore " << err_msg;
                throw limestone_exception(exception_type::initialization_failure, err_msg);
            }
            std::string err_msg = "failed to acquire lock for manifest in directory: " + location_.string();
            LOG(FATAL) << "/:limestone:config:datastore " << err_msg;
            throw limestone_io_exception(exception_type::initialization_failure, err_msg, errno);
        }

        add_file(compaction_catalog_path);
        compaction_catalog_ = std::make_unique<compaction_catalog>(compaction_catalog::from_catalog_file(location_));

        epoch_file_path_ = location_ / std::string(limestone::internal::epoch_file_name);
        tmp_epoch_file_path_ = location_ / std::string(limestone::internal::tmp_epoch_file_name);
        const bool result = boost::filesystem::exists(epoch_file_path_, error);
        if (!result || error) {
            FILE* strm = fopen(epoch_file_path_.c_str(), "a");  // NOLINT(*-owning-memory)
            if (!strm) {
                LOG_AND_THROW_IO_EXCEPTION("does not have write permission for the log_location directory, path: " + location_.string(), errno);
            }
            if (fclose(strm) != 0) {  // NOLINT(*-owning-memory)
                LOG_AND_THROW_IO_EXCEPTION("fclose failed", errno);
            }
            add_file(epoch_file_path_);
        }

        const bool exists = boost::filesystem::exists(tmp_epoch_file_path_, error);        
        if (exists) {
            const bool result_remove = boost::filesystem::remove(tmp_epoch_file_path_, error);
            if (!result_remove || error) {
                LOG_AND_THROW_IO_EXCEPTION("fail to remove temporary epoch file, path: " + tmp_epoch_file_path_.string(), errno);
            }
        }

        recover_max_parallelism_ = conf.recover_max_parallelism_;
        LOG(INFO) << "/:limestone:config:datastore setting the number of recover process thread = " << recover_max_parallelism_;

        VLOG_LP(log_debug) << "datastore is created, location = " << location_.string();
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

datastore::~datastore() noexcept{
    stop_online_compaction_worker();
    if (online_compaction_worker_future_.valid()) {
        online_compaction_worker_future_.wait();
    }
}


void datastore::recover() const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

enum class file_write_mode {
    append,
    overwrite
};

static void write_epoch_to_file_internal(const std::string& file_path, epoch_id_type epoch_id, file_write_mode mode) {
    const char* fopen_mode = (mode == file_write_mode::append) ? "a" : "w";
    std::unique_ptr<FILE, void (*)(FILE*)> file_ptr(fopen(file_path.c_str(), fopen_mode), [](FILE* fp) {
        if (fp) {
            if (fclose(fp) != 0) { // NOLINT(cppcoreguidelines-owning-memory)
                LOG_AND_THROW_IO_EXCEPTION("fclose failed", errno);
            }
        }
    });  
    if (!file_ptr) {
        LOG_AND_THROW_IO_EXCEPTION("fopen failed for file: " + file_path, errno);
    }

    log_entry::durable_epoch(file_ptr.get(), epoch_id);

    if (fflush(file_ptr.get()) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fflush failed for file: " + file_path, errno);
    }
    if (fsync(fileno(file_ptr.get())) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fsync failed for file: " + file_path, errno);
    }
}

void datastore::write_epoch_to_file(epoch_id_type epoch_id) {
    TRACE_START << "epoch_id=" << epoch_id;
    if (++epoch_write_counter >= max_entries_in_epoch_file) {
        write_epoch_to_file_internal(tmp_epoch_file_path_.string(), epoch_id, file_write_mode::overwrite);

        boost::system::error_code ec;
        if (::rename(tmp_epoch_file_path_.c_str(), epoch_file_path_.c_str()) != 0) {
            TRACE_ABORT;
            LOG_AND_THROW_IO_EXCEPTION("Failed to rename temp file: " + tmp_epoch_file_path_.string() + " to " + epoch_file_path_.string(), errno);
        }
        boost::filesystem::remove(tmp_epoch_file_path_, ec);
        if (ec) {
            TRACE_ABORT;
            LOG_AND_THROW_IO_EXCEPTION("Failed to remove temp file: " + tmp_epoch_file_path_.string(), ec);
        }
        epoch_write_counter = 0;
    } else {
        write_epoch_to_file_internal(epoch_file_path_.string(), epoch_id, file_write_mode::append);
    }
    TRACE_END;
}



void datastore::ready() {
    try {
        create_snapshot();
        online_compaction_worker_future_ = std::async(std::launch::async, &datastore::online_compaction_worker, this);
        if (epoch_id_switched_.load() != 0) {
            write_epoch_to_file(epoch_id_informed_.load());
        }
        cleanup_rotated_epoch_files(location_);
        state_ = state::ready;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

std::unique_ptr<snapshot> datastore::get_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::unique_ptr<snapshot>(new snapshot(location_, clear_storage));
}

std::shared_ptr<snapshot> datastore::shared_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::shared_ptr<snapshot>(new snapshot(location_, clear_storage));
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
    TRACE_START << "new_epoch_id=" << new_epoch_id;
    try {
        check_after_ready(static_cast<const char*>(__func__));
        auto neid = static_cast<std::uint64_t>(new_epoch_id);
        if (auto switched = epoch_id_switched_.load(); neid <= switched) {
            LOG_LP(WARNING) << "switch to epoch_id_type of " << neid << " (<=" << switched << ") is curious";
        }

        epoch_id_switched_.store(neid);
        if (state_ != state::not_ready) {
            update_min_epoch_id(true);
        }
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
}

void datastore::update_min_epoch_id(bool from_switch_epoch) {  // NOLINT(readability-function-cognitive-complexity)
    TRACE_START << "from_switch_epoch=" << from_switch_epoch;
    auto upper_limit = epoch_id_switched_.load();
    if (upper_limit == 0) {
        return; // If epoch_id_switched_ is zero, it means no epoch has been switched, so updating epoch_id_to_be_recorded_ and epoch_id_informed_ is unnecessary.
    }
    upper_limit--;

    epoch_id_type max_finished_epoch = 0;

    for (const auto& e : log_channels_) {
        auto working_epoch = e->current_epoch_id_.load();
        auto finished_epoch = e->finished_epoch_id_.load();
        if (working_epoch > finished_epoch && working_epoch != UINT64_MAX) {
            if ((working_epoch - 1) < upper_limit) {
                upper_limit = working_epoch - 1;
            }
        }
        if (max_finished_epoch < finished_epoch && max_finished_epoch <= upper_limit) {
            max_finished_epoch = finished_epoch;
        }
    }

    TRACE << "epoch_id_switched_ = " << epoch_id_switched_.load() << ", upper_limit = " << upper_limit << ", max_finished_epoch = " << max_finished_epoch;

    // update recorded_epoch_
    auto to_be_epoch = upper_limit;
    if (to_be_epoch > static_cast<std::uint64_t>(max_finished_epoch)) {
        to_be_epoch = static_cast<std::uint64_t>(max_finished_epoch);
    }

    TRACE << "update epoch file part start with to_be_epoch = " << to_be_epoch;
    auto old_epoch_id = epoch_id_to_be_recorded_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_to_be_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            TRACE << "epoch_id_to_be_recorded_ updated to " << to_be_epoch;
            std::lock_guard<std::mutex> lock(mtx_epoch_file_);
            if (to_be_epoch < epoch_id_to_be_recorded_.load()) {
                break;
            }           
            write_epoch_to_file(static_cast<epoch_id_type>(to_be_epoch));
            epoch_id_record_finished_.store(to_be_epoch);
            TRACE << "epoch_id_record_finished_ updated to " << to_be_epoch;
            break;
        }
    }
    if (to_be_epoch > epoch_id_record_finished_.load()) {
        TRACE << "skipping persistent callback part, to_be_epoch =  " << to_be_epoch << ", epoch_id_record_finished_ = " << epoch_id_record_finished_.load();
        TRACE_END;
        return;
    }

    // update informed_epoch_
    to_be_epoch = upper_limit;
    TRACE << "persistent callback part start with to_be_epoch =" << to_be_epoch;
    // In `informed_epoch_`, the update restriction based on the `from_switch_epoch` condition is intentionally omitted.
    // Due to the interface specifications of Shirakami, it is necessary to advance the epoch even if the log channel
    // is not updated. This behavior differs from `recorded_epoch_` and should be maintained as such.
    old_epoch_id = epoch_id_informed_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_informed_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            TRACE << "epoch_id_informed_ updated to " << to_be_epoch;
            {
                std::lock_guard<std::mutex> lock(mtx_epoch_persistent_callback_);
                if (to_be_epoch < epoch_id_informed_.load()) {
                    break;
                }
                if (persistent_callback_) {
                    TRACE <<  "start calling persistent callback to " << to_be_epoch;
                    persistent_callback_(to_be_epoch);
                    TRACE <<  "end calling persistent callback to " << to_be_epoch;
                }
            }
            {
                // Notify waiting threads in rotate_log_files() about the update to epoch_id_informed_
                std::lock_guard<std::mutex> lock(informed_mutex);
                cv_epoch_informed.notify_all();  
            }
            break;
        }
    }
    TRACE_END;
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

    stop_online_compaction_worker();
    if (!online_compaction_worker_future_.valid()) {
        VLOG(log_info) << "/:limestone:datastore:shutdown compaction task is not running. skipping task shutdown.";
    } else {
        VLOG(log_info) << "/:limestone:datastore:shutdown shutdown: waiting for compaction task to stop";
        online_compaction_worker_future_.wait();
        VLOG(log_info) << "/:limestone:datastore:shutdown compaction task has been stopped.";
    }

    if (fd_for_flock_ != -1) {
        if (::close(fd_for_flock_) == -1) {
            VLOG(log_error) << "Failed to close lock file descriptor: " << strerror(errno);
        } else {
            fd_for_flock_ = -1;
        }
    }

    return std::async(std::launch::async, [] {
        std::this_thread::sleep_for(std::chrono::microseconds(100000));
        VLOG(log_info) << "/:limestone:datastore:shutdown end";
    });
}

// old interface
backup& datastore::begin_backup() {
    try {
        auto tmp_files = get_files();
        backup_ = std::unique_ptr<backup>(new backup(tmp_files));
        return *backup_;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
    return *backup_; // Required to satisfy the compiler
}

std::unique_ptr<backup_detail> datastore::begin_backup(backup_type btype) {  // NOLINT(readability-function-cognitive-complexity)
    try {
        rotate_epoch_file();
        rotation_result result = rotate_log_files();

        // LOG-0: all files are log file, so all files are selected in both standard/transaction mode.
        (void) btype;

        // calculate files_ minus active-files
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
                    }
                    break;
                }
                default: {
                    // unknown type
                }
            }
        }
        return std::unique_ptr<backup_detail>(new backup_detail(entries, epoch_id_switched_.load()));
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
        throw; // Unreachable, but required to satisfy the compiler
    }
}

tag_repository& datastore::epoch_tag_repository() noexcept {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] const epoch_tag& tag) const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

rotation_result datastore::rotate_log_files() {
    TRACE_START;
    std::lock_guard<std::mutex> lock(rotate_mutex); 
    TRACE << "start rotate_log_files() critical section";
    auto epoch_id = epoch_id_switched_.load();
    if (epoch_id == 0) {
        LOG_AND_THROW_EXCEPTION("rotation requires epoch_id > 0, but got epoch_id = 0");
    }
    TRACE << "epoch_id = " << epoch_id;
    {
        on_wait1(); // for testing
        // Wait until epoch_id_informed_ is less than rotated_epoch_id to ensure safe rotation.
        std::unique_lock<std::mutex> ul(informed_mutex);
        while (epoch_id_informed_.load() < epoch_id) {
            cv_epoch_informed.wait(ul);  
        }
    }
    TRACE << "end waiting for epoch_id_informed_ to catch up";
    rotation_result result(epoch_id);
    for (const auto& lc : log_channels_) {
        boost::system::error_code error;
        bool ret = boost::filesystem::exists(lc->file_path(), error);
        if (!ret || error) {
            continue;  // skip if not exists
        }
        std::string rotated_file = lc->do_rotate_file();
        result.add_rotated_file(rotated_file);
    }
    result.set_rotation_end_files(get_files());
    TRACE_END;
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
    boost::system::error_code ec;
    boost::filesystem::rename(epoch_file_path_, new_file, ec);
    if (ec) {
        std::string err_msg = "Failed to rename epoch_file from " + epoch_file_path_.string() + " to " + new_file.string();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
    add_file(new_file);

    // create new one
    boost::filesystem::ofstream strm{};
    strm.open(epoch_file_path_, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    if(!strm || !strm.is_open() || strm.bad() || strm.fail()){
        THROW_LIMESTONE_IO_EXCEPTION("does not have write permission for the log_location directory, path: " + location_.string(), errno);
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
    LOG_LP(INFO) << "online compaction worker started..." << std::endl;

    boost::filesystem::path ctrl_dir = location_ / "ctrl";
    boost::filesystem::path start_file = ctrl_dir / "start_compaction";


    ensure_directory_exists(ctrl_dir);
    if (!boost::filesystem::exists(ctrl_dir)) {
        if (!boost::filesystem::create_directory(ctrl_dir)) {
            LOG_LP(INFO) << "failed to create directory: " << ctrl_dir.string();
            return;
        } 
    }

    std::unique_lock<std::mutex> lock(mtx_online_compaction_worker_);

    while (!stop_online_compaction_worker_.load()) {
        if (boost::filesystem::exists(start_file)) {
            if (!boost::filesystem::remove(start_file)) {
                LOG_LP(ERROR) << "failed to remove file: " << start_file.string();
                return;
            }
            try {
                compact_with_online();
            } catch (const limestone_exception& e) {
                LOG_LP(ERROR) << "failed to compact with online: " << e.what();
            }
        }
        cv_online_compaction_worker_.wait_for(lock, std::chrono::seconds(1), [this]() {
            return stop_online_compaction_worker_.load();
        });
    }
}

void datastore::stop_online_compaction_worker() {
    {
        std::lock_guard<std::mutex> lock(mtx_online_compaction_worker_);
        stop_online_compaction_worker_.store(true);
    }
    cv_online_compaction_worker_.notify_all();
}

void datastore::compact_with_online() {
    TRACE_START;
    check_after_ready(static_cast<const char*>(__func__));

    // rotate first
    rotation_result result = rotate_log_files();

    // select files for compaction
    std::set<std::string> detached_pwals = compaction_catalog_->get_detached_pwals();

    
    for (const auto& filename : detached_pwals) {
        LOG_LP(INFO) << "detached_pwals:" << filename;
    }


    std::set<std::string> need_compaction_filenames = select_files_for_compaction(result.get_rotation_end_files(), detached_pwals);
    if (need_compaction_filenames.empty() ||
        (need_compaction_filenames.size() == 1 &&
         need_compaction_filenames.find(compaction_catalog::get_compacted_filename()) != need_compaction_filenames.end())) {
        LOG_LP(INFO) << "no files to compact";
        TRACE_END << "return compact_with_online() without compaction";
        return;
    }

    for (const auto& filename : need_compaction_filenames) {
        LOG_LP(INFO) << "need_compaction_filenames: " << filename;
    }

    // create a temporary directory for online compaction
    boost::filesystem::path compaction_temp_dir = location_ / compaction_catalog::get_compaction_temp_dirname();
    ensure_directory_exists(compaction_temp_dir);

    // create a compacted file
    create_compact_pwal(location_, compaction_temp_dir, recover_max_parallelism_, need_compaction_filenames);

    // handle existing compacted file
    handle_existing_compacted_file(location_);

    // move pwal_0000.compacted from the temp directory to the log directory
    boost::filesystem::path compacted_file = location_ / compaction_catalog::get_compacted_filename();
    boost::filesystem::path temp_compacted_file = compaction_temp_dir / compaction_catalog::get_compacted_filename();
    safe_rename(temp_compacted_file, compacted_file);

    // get a set of all files in the location_ directory
    std::set<std::string> files_in_location = get_files_in_directory(location_);
    
    // check if detached_pwals exist in location_
    for (auto it = detached_pwals.begin(); it != detached_pwals.end();) {
        if (files_in_location.find(*it) == files_in_location.end()) {
            LOG_LP(WARNING) << "File " << *it << " does not exist in the directory and will be removed from detached_pwals.";
            auto p = location_ / *it;
            subtract_file(p);
            it = detached_pwals.erase(it);  // Erase and move to the next iterator
        } else {
            ++it;  // Move to the next iterator
        }
    }


    // update compaction catalog
    compacted_file_info compacted_file_info{compacted_file.filename().string(), 1};
    detached_pwals.erase(compacted_file.filename().string());
    compaction_catalog_->update_catalog_file(result.get_epoch_id(), {compacted_file_info}, detached_pwals);
    add_file(compacted_file);

    // remove pwal_0000.compacted.prev
    remove_file_safely(location_ / compaction_catalog::get_compacted_backup_filename());

    LOG_LP(INFO) << "compaction finished";
    TRACE_END;
}

} // namespace limestone::api

