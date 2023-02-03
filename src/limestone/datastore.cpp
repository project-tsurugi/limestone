/*
 * Copyright 2022-2023 tsurugi project.
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

#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/backup_detail.h>

#include "datastore.h"
#include "backup.h"
#include "log_entry.h"
#include "snapshot.h"

namespace limestone::api::impl {

datastore::datastore() noexcept = default;

datastore::datastore(api::configuration const& conf) {
    location_ = conf.data_locations().at(0);
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(location_, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(location_, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory: result_mkdir: " << result_mkdir << ", error_code: " << error << ", path: " << location_;
            throw std::runtime_error("fail to create the log_location directory");  //NOLINT
        }
    } else {
        BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(location_), boost::filesystem::directory_iterator())) {
            if (!boost::filesystem::is_directory(p)) {
                add_file(p);
            }
        }
    }

    // XXX: prusik era
    // TODO: read rotated epoch files if main epoch file does not exist
    epoch_file_path_ = location_ / boost::filesystem::path(std::string(epoch_file_name));
    const bool result = boost::filesystem::exists(epoch_file_path_, error);
    if (!result || error) {
        boost::filesystem::ofstream strm{};
        strm.open(epoch_file_path_, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
        if(!strm || !strm.is_open() || strm.bad() || strm.fail()){
            LOG_LP(ERROR) << "does not have write permission for the log_location directory, path: " <<  location_;
            throw std::runtime_error("does not have write permission for the log_location directory");  //NOLINT
        }
        strm.close();
        add_file(epoch_file_path_);
    }
    
    VLOG_LP(log_debug) << "datastore is created, location = " << location_.string();
}

datastore::~datastore() noexcept = default;

void datastore::recover() const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

void datastore::ready() noexcept {
    create_snapshot();
    state_ = state::ready;
}

std::unique_ptr<api::snapshot> datastore::get_snapshot() const noexcept {
    check_after_ready(static_cast<const char*>(__func__));
    return std::unique_ptr<snapshot>(new snapshot(location_));
}

std::shared_ptr<api::snapshot> datastore::shared_snapshot() const noexcept {
    check_after_ready(static_cast<const char*>(__func__));
    return std::shared_ptr<snapshot>(new snapshot(location_));
}

api::log_channel& datastore::create_channel(const boost::filesystem::path& location) noexcept {
    check_before_ready(static_cast<const char*>(__func__));
    
    std::lock_guard<std::mutex> lock(mtx_channel_);
    
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(std::unique_ptr<log_channel>(new log_channel(location, id, *this)));  // constructor of log_channel is private
    return *log_channels_.at(id);
}

epoch_id_type datastore::last_epoch() const noexcept { return static_cast<epoch_id_type>(epoch_id_informed_.load()); }

void datastore::switch_epoch(epoch_id_type new_epoch_id) noexcept {
    check_after_ready(static_cast<const char*>(__func__));

    auto neid = static_cast<std::uint64_t>(new_epoch_id);
    if (neid <= epoch_id_switched_.load()) {
        LOG_LP(WARNING) << "switch to epoch_id_type of " << neid << " is curious";
    }

    epoch_id_switched_.store(neid);
    update_min_epoch_id(true);
}

void datastore::update_min_epoch_id(bool from_switch_epoch) noexcept {
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

    auto to_be_epoch = upper_limit;
    auto old_epoch_id = epoch_id_informed_.load();
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

    if (from_switch_epoch && (to_be_epoch > static_cast<std::uint64_t>(max_finished_epoch))) {
        to_be_epoch = static_cast<std::uint64_t>(max_finished_epoch);
    }
    old_epoch_id = epoch_id_recorded_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        if (epoch_id_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            std::lock_guard<std::mutex> lock(mtx_epoch_file_);

            boost::filesystem::ofstream strm{};
            strm.open(epoch_file_path_, std::ios_base::out | std::ios_base::app | std::ios_base::binary );
            log_entry::durable_epoch(strm, static_cast<epoch_id_type>(epoch_id_informed_.load()));
            strm.close();
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
    state_ = state::shutdown;
    return std::async(std::launch::async, []{ std::this_thread::sleep_for(std::chrono::microseconds(100000)); });
}

// old interface
api::backup& datastore::begin_backup() noexcept {
    backup_ = std::unique_ptr<backup>(new backup(files_));
    return *backup_;
}

std::unique_ptr<api::backup_detail> datastore::begin_backup(backup_type btype) {
    rotate_log_files();

    // LOG-0: all files are log file, so all files are selected in both standard/transaction mode.
    (void) btype;

    std::vector<backup_detail::entry> entries;
    for (auto & ent : files_) {
        // XXX: skip active files
        // LOG-0: assume files are located flat in logdir.
        auto filename = ent.filename().string();
        auto dst = filename;
        switch (filename[0]) {
            case 'p': {
                if (filename.find("wal", 1) == 1) {
                    // "pwal"
                    // pwal files are type:logfile, detached

                    // skip active file
                    if (filename.length() == 9) {  // FIXME: too adohoc check
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
            default: {
                // unknown type
            }
        }
    }
    return std::unique_ptr<backup_detail>(new api::backup_detail(entries, epoch_id_switched_.load()));
}

api::tag_repository& datastore::epoch_tag_repository() noexcept {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] const epoch_tag& tag) const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

epoch_id_type datastore::rotate_log_files() {
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
        if (!lc->registered_) {
            continue;
        }
        lc->do_rotate_file();
    }
    rotate_epoch_file();

    return epoch_id_switched_.load();
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
        throw std::runtime_error("does not have write permission for the log_location directory");  //NOLINT
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

} // namespace limestone::api::impl

namespace limestone::api {
/**
 * @brief factory method for datastore with the given configuration
 * @param conf a reference to a configuration object used in the object construction
 */
std::unique_ptr<datastore> create_datastore(configuration const& conf) {
    return std::make_unique<impl::datastore>(conf);
}
} // namespace limestone::api