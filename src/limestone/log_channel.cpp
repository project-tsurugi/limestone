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

#include <chrono>
#include <sstream>
#include <iomanip>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"

#include <limestone/api/log_channel.h>
#include <limestone/api/datastore.h>
#include "internal.h"
#include "log_entry.h"
#include "rotation_task.h"

namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location, std::size_t id, datastore& envelope) noexcept
    : envelope_(envelope), location_(std::move(location)), id_(id)
{
    std::stringstream ss;
    ss << limestone::internal::log_channel_prefix << std::setw(4) << std::setfill('0') << std::dec << id_;
    file_ = ss.str();
}

void log_channel::begin_session() {
    try {
        do {
            current_epoch_id_.store(envelope_.epoch_id_switched_.load());
            std::atomic_thread_fence(std::memory_order_acq_rel);
        } while (current_epoch_id_.load() != envelope_.epoch_id_switched_.load());
        latest_session_epoch_id_.store(static_cast<epoch_id_type>(current_epoch_id_.load()));

        auto log_file = file_path();
        strm_ = fopen(log_file.c_str(), "a");  // NOLINT(*-owning-memory)
        if (!strm_) {
            LOG_AND_THROW_IO_EXCEPTION("cannot make file on " + location_.string(), errno);
        }
        setvbuf(strm_, nullptr, _IOFBF, 128L * 1024L);  // NOLINT, NB. glibc may ignore size when _IOFBF and buffer=NULL
        if (!registered_) {
            envelope_.add_file(log_file);
            registered_ = true;
        }
        log_entry::begin_session(strm_, static_cast<epoch_id_type>(current_epoch_id_.load()));
        {
            std::lock_guard<std::mutex> lock(session_mutex_);
            waiting_epoch_ids_.insert(latest_session_epoch_id_);
        }
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::end_session() {
    try {
        if (fflush(strm_) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("fflush failed", errno);
        }
        if (fsync(fileno(strm_)) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("fsync failed", errno);
        }
        finished_epoch_id_.store(current_epoch_id_.load());
        current_epoch_id_.store(UINT64_MAX);
        envelope_.update_min_epoch_id();

        if (fclose(strm_) != 0) {  // NOLINT(*-owning-memory)
            LOG_AND_THROW_IO_EXCEPTION("fclose failed", errno);
        }

        // Remove current_epoch_id_ from waiting_epoch_ids_
        {
            std::lock_guard<std::mutex> lock(session_mutex_);
            waiting_epoch_ids_.erase(latest_session_epoch_id_.load());
            // Notify waiting threads
            session_cv_.notify_all();
        }
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::abort_session([[maybe_unused]] status status_code, [[maybe_unused]] const std::string& message) noexcept {
    try {
        LOG_LP(ERROR) << "not implemented";
        std::abort();  // FIXME
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::add_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
    try {
        log_entry::write(strm_, storage_id, key, value, write_version);
        write_version_ = write_version;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::add_entry([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value, [[maybe_unused]] write_version_type write_version, [[maybe_unused]] const std::vector<large_object_input>& large_objects) {
    LOG_AND_THROW_EXCEPTION("not implemented");// FIXME
};

void log_channel::remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version) {
    try {
        log_entry::write_remove(strm_, storage_id, key, write_version);
        write_version_ = write_version;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::add_storage(storage_id_type storage_id, write_version_type write_version) {
    try {
        log_entry::write_add_storage(strm_, storage_id, write_version);
        write_version_ = write_version;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::remove_storage(storage_id_type storage_id, write_version_type write_version) {
    try {
        log_entry::write_remove_storage(strm_, storage_id, write_version);
        write_version_ = write_version;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::truncate_storage(storage_id_type storage_id, write_version_type write_version) {
    try {
        log_entry::write_clear_storage(strm_, storage_id, write_version);
        write_version_ = write_version;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

boost::filesystem::path log_channel::file_path() const noexcept {
    return location_ / file_;
}

// DO rotate without condition check.
//  use this after your check
rotation_result log_channel::do_rotate_file(epoch_id_type epoch) {
    std::stringstream ss;
    ss << file_.string() << "."
       << std::setw(14) << std::setfill('0') << envelope_.current_unix_epoch_in_millis()
       << "." << epoch;
    std::string new_name = ss.str();
    boost::filesystem::path new_file = location_ / new_name;
    boost::system::error_code ec;
    boost::filesystem::rename(file_path(), new_file, ec);
    if (ec) {
        std::string err_msg = "Failed to rename file from " + file_path().string() + " to " + new_file.string() + ". Error: " + ec.message();
        LOG_AND_THROW_IO_EXCEPTION(err_msg, ec);
    }
    envelope_.add_file(new_file);

    registered_ = false;
    envelope_.subtract_file(location_ / file_);

    // Create a rotation result with the current epoch ID
    rotation_result result(new_name, latest_session_epoch_id_);
    return result;
}

void log_channel::wait_for_end_session(epoch_id_type epoch) {
    std::unique_lock<std::mutex> lock(session_mutex_);

    // Wait until the specified epoch_id is removed from waiting_epoch_ids_
    session_cv_.wait(lock, [this, epoch]() {
        // Ensure that no ID less than or equal to the specified epoch exists in waiting_epoch_ids_
        return waiting_epoch_ids_.empty() || *waiting_epoch_ids_.begin() > epoch;
    });
}

} // namespace limestone::api
