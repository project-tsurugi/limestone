/*
 * Copyright 2022-2022 tsurugi project.
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

#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>

#include <limestone/api/datastore.h>
#include "log_entry.h"

namespace limestone::api {

datastore::datastore() noexcept = default;

datastore::datastore(configuration const& conf) noexcept {
    location_ = conf.data_locations_.at(0);
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(location_, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(location_, error);
        if (!result_mkdir || error) {
            LOG(ERROR) << "fail to create directory: result_mkdir: " << result_mkdir << ", error_code: " << error << ", path: " << location_;
            std::abort();
        }
    }
    epoch_file_path_ = location_ / boost::filesystem::path(std::string(epoch_file_name));
    add_file(epoch_file_path_);
    DVLOG(log_debug) << "datastore is created, location = " << location_.string();
}

datastore::~datastore() noexcept = default;

void datastore::recover() const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

void datastore::ready() noexcept {
    create_snapshot();
    state_ = state::ready;
}

std::unique_ptr<snapshot> datastore::get_snapshot() const noexcept {
    check_after_ready(static_cast<const char*>(__func__));
    return std::unique_ptr<snapshot>(new snapshot(location_));
}

std::shared_ptr<snapshot> datastore::shared_snapshot() const noexcept {
    check_after_ready(static_cast<const char*>(__func__));
    return std::shared_ptr<snapshot>(new snapshot(location_));
}

log_channel& datastore::create_channel(const boost::filesystem::path& location) noexcept {
    check_before_ready(static_cast<const char*>(__func__));
    
    std::lock_guard<std::mutex> lock(mtx_channel_);
    
    auto id = log_channel_id_.fetch_add(1);
    log_channels_.emplace_back(std::unique_ptr<log_channel>(new log_channel(location, id, *this)));  // contrructor of log_channel is private
    return *log_channels_.at(id);
}

epoch_id_type datastore::last_epoch() const noexcept { return static_cast<epoch_id_type>(epoch_id_informed_.load()); }

void datastore::switch_epoch(epoch_id_type new_epoch_id) noexcept {
    check_after_ready(static_cast<const char*>(__func__));

    auto neid = static_cast<std::uint64_t>(new_epoch_id);
    if (neid <= epoch_id_switched_.load()) {
        LOG(WARNING) << "switch to epoch_id_type of " << neid << " is curious";
    }

    epoch_id_switched_.store(neid);
    if (epoch_id_informed_.load() < (neid - 1)) {
        update_min_epoch_id(false);
    }
}

bool datastore::update_min_epoch_id(bool update_finished) noexcept {
    auto upper_limit = epoch_id_switched_.load() - 1;
    std::uint64_t max_finished_epoch = 0;

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
    std::uint64_t old_epoch_id = epoch_id_informed_.load();
    std::uint64_t to_be_informed_epoch = old_epoch_id;
    if (update_finished && (to_be_informed_epoch < max_finished_epoch)) {
        to_be_informed_epoch = max_finished_epoch;
    }
    if (to_be_informed_epoch < upper_limit) {
        to_be_informed_epoch = upper_limit;
    }

    while (true) {
        if (bool rv = (old_epoch_id < to_be_informed_epoch); rv) {
            if (epoch_id_informed_.compare_exchange_strong(old_epoch_id, to_be_informed_epoch)) {
                if (persistent_callback_) {
                    persistent_callback_(to_be_informed_epoch);
                }
                if (update_finished && rv) {
                    std::lock_guard<std::mutex> lock(mtx_epoch_file_);

                    boost::filesystem::ofstream strm{};
                    strm.open(epoch_file_path_, std::ios_base::out | std::ios_base::app | std::ios_base::binary );
                    log_entry::durable_epoch(strm, static_cast<epoch_id_type>(epoch_id_informed_.load()));
                    strm.close();
                }
                return rv;
            }
        }
        return false;
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

backup& datastore::begin_backup() noexcept {
    backup_ = std::unique_ptr<backup>(new backup(files_));
    return *backup_;
}

tag_repository& datastore::epoch_tag_repository() noexcept {
    return tag_repository_;
}

void datastore::recover([[maybe_unused]] const epoch_tag& tag) const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}

void datastore::add_file(const boost::filesystem::path& file) noexcept {
    std::lock_guard<std::mutex> lock(mtx_files_);

    files_.insert(file);
}

void datastore::check_after_ready(std::string_view func) const noexcept {
    if (state_ == state::not_ready) {
        DVLOG(log_debug) << func << " called before ready()";
    }
}

void datastore::check_before_ready(std::string_view func) const noexcept {
    if (state_ != state::not_ready) {
        DVLOG(log_debug) << func << " called after ready()";
    }
}

} // namespace limestone::api
