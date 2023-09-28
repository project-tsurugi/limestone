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

#include <chrono>
#include <sstream>
#include <iomanip>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/log_channel.h>

#include <limestone/api/datastore.h>
#include "log_entry.h"

namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location, std::size_t id, datastore& envelope) noexcept
    : envelope_(envelope), location_(std::move(location)), id_(id)
{
    std::stringstream ss;
    ss << prefix << std::setw(4) << std::setfill('0') << std::dec << id_;
    file_ = ss.str();
}

void log_channel::begin_session() noexcept {
    do {
        current_epoch_id_.store(envelope_.epoch_id_switched_.load());
        std::atomic_thread_fence(std::memory_order_acq_rel);
    } while (current_epoch_id_.load() != envelope_.epoch_id_switched_.load());

    auto log_file = file_path();
    strm_ = fopen(log_file.c_str(), "a");  // NOLINT TODO: error check
    setvbuf(strm_, nullptr, _IOFBF, 1024L * 1024L);  // NOLINT TODO: error check
    if (!registered_) {
        envelope_.add_file(log_file);
        registered_ = true;
    }
    log_entry::begin_session(strm_, static_cast<epoch_id_type>(current_epoch_id_.load()));
}

void log_channel::end_session() noexcept {
    fflush(strm_);  // NOLINT TODO: error check
    if (int rc = fsync(fileno(strm_)); rc != 0) {
        LOG_LP(ERROR) << "fsync failed, errno = " << errno;
        std::abort();
    }
    finished_epoch_id_.store(current_epoch_id_.load());
    current_epoch_id_.store(UINT64_MAX);
    envelope_.update_min_epoch_id();
    fclose(strm_);  // NOLINT TODO: error check
}

void log_channel::abort_session([[maybe_unused]] status status_code, [[maybe_unused]] const std::string& message) noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME
}

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) noexcept {
    log_entry::write(strm_, storage_id, key, value, write_version);
    write_version_ = write_version;
}

void log_channel::add_entry([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value, [[maybe_unused]] write_version_type write_version, [[maybe_unused]] const std::vector<large_object_input>& large_objects) noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME
};

void log_channel::remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version) noexcept {
    log_entry::write_remove(strm_, storage_id, key, write_version);
    write_version_ = write_version;
}

void log_channel::add_storage([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] write_version_type write_version) noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME
}

void log_channel::remove_storage([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] write_version_type write_version) noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME
}

void log_channel::truncate_storage([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] write_version_type write_version) noexcept {
    LOG_LP(ERROR) << "not implemented";
    std::abort();  // FIXME
}

boost::filesystem::path log_channel::file_path() const noexcept {
    return location_ / file_;
}

// DO rotate without condition check.
//  use this after your check
void log_channel::do_rotate_file(epoch_id_type epoch) {
    // XXX: multi-thread broken

    std::stringstream ss;
    ss << file_.string() << "."
       << std::setw(14) << std::setfill('0') << envelope_.current_unix_epoch_in_millis()
       << "." << epoch;
    std::string new_name = ss.str();
    boost::filesystem::path new_file = location_ / new_name;
    boost::filesystem::rename(file_path(), new_file);
    envelope_.add_file(new_file);

    envelope_.subtract_file(location_ / file_);
    registered_ = false;
}

} // namespace limestone::api
