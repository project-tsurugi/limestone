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

#include <glog/logging.h>
#include <limestone/api/datastore.h>
#include <limestone/api/log_channel.h>
#include <limestone/logging.h>

#include <chrono>
#include <future>
#include <iomanip>
#include <sstream>
#include <thread>

#include "internal.h"
#include "datastore_impl.h"
#include "limestone_exception_helper.h"
#include "log_channel_impl.h"
#include "log_entry.h"
#include "logging_helper.h"
#include "replication/message_log_entries.h"
namespace limestone::api {

log_channel::log_channel(boost::filesystem::path location, std::size_t id, datastore& envelope) noexcept
    : envelope_(envelope), location_(std::move(location)), id_(id)
{
    std::stringstream ss;
    ss << limestone::internal::log_channel_prefix << std::setw(4) << std::setfill('0') << std::dec << id_;
    file_ = ss.str();
    impl_ = std::make_unique<log_channel_impl>();
}

void log_channel::begin_session() {
    try {
        // Synchronize `current_epoch_id_` with `epoch_id_switched_`.
        // This loop is necessary to prevent inconsistencies in `current_epoch_id_`
        // that could occur if `epoch_id_switched_` changes at a specific timing.
        //
        // Case where inconsistency occurs:
        // 1. This thread (L) loads `epoch_id_switched_` and reads 10.
        // 2. Another thread (S) immediately updates `epoch_id_switched_` to 11.
        // 3. If the other thread (S) reads `current_epoch_id_` at this point,
        //    it expects `current_epoch_id_` to be consistent with the latest
        //    `epoch_id_switched_` value (11), but `current_epoch_id_` may still
        //    hold the outdated value, causing an inconsistency.
        //
        // This loop detects such inconsistencies and repeats until `current_epoch_id_`
        // matches the latest value of `epoch_id_switched_`, ensuring consistency.
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while)
        do {
            envelope_.on_begin_session_current_epoch_id_store(); // for testing
            current_epoch_id_.store(envelope_.epoch_id_switched_.load());
            std::atomic_thread_fence(std::memory_order_acq_rel);
        } while (current_epoch_id_.load() != envelope_.epoch_id_switched_.load());
        TRACE_START << "current_epoch_id_=" << current_epoch_id_.load();

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
        uint64_t epoch_id = current_epoch_id_.load();
        log_entry::begin_session(strm_, static_cast<epoch_id_type>(epoch_id));
        impl_->send_replica_message(epoch_id, [&](replication::message_log_entries &msg) {
            msg.set_session_begin_flag(true);
        });
        TRACE_END;
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

void log_channel::finalize_session_file() {
    uint64_t epoch_id = current_epoch_id_.load();
    log_entry::end_session(strm_, static_cast<epoch_id_type>(epoch_id));
    if (fflush(strm_) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fflush failed", errno);
    }
    if (fsync(fileno(strm_)) != 0) {
        LOG_AND_THROW_IO_EXCEPTION("fsync failed", errno);
    }
    envelope_.on_end_session_finished_epoch_id_store(); // for testing
    finished_epoch_id_.store(current_epoch_id_.load());
    envelope_.update_min_epoch_id();
    envelope_.on_end_session_current_epoch_id_store(); // for testing
    current_epoch_id_.store(UINT64_MAX);

    if (fclose(strm_) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("fclose failed", errno);
    }
}

void log_channel::end_session() {
    try {
        TRACE_START << "current_epoch_id_=" << current_epoch_id_.load();
        if (envelope_.impl_->is_async_session_close_enabled()) {
            bool sent = impl_->send_replica_message(finished_epoch_id_.load(), [&](replication::message_log_entries &msg) {
                msg.set_session_end_flag(true);
                msg.set_flush_flag(true);
            });
            finalize_session_file();
            if (sent) {
                impl_->wait_for_replica_ack();
            }
        } else {
            finalize_session_file();
            bool sent = impl_->send_replica_message(finished_epoch_id_.load(), [&](replication::message_log_entries &msg) {
                msg.set_session_end_flag(true);
                msg.set_flush_flag(true);
            });
            if (sent) {
                impl_->wait_for_replica_ack();
            }
        }
        TRACE_END;
    } catch (...) {
        TRACE_ABORT;
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
    TRACE_START << "storage_id=" << storage_id << ", key=" << key << ",value = " << value << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    try {
        log_entry::write(strm_, storage_id, key, value, write_version);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_normal_entry(storage_id, key, value, write_version);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
}

void log_channel::add_entry([[maybe_unused]] storage_id_type storage_id, [[maybe_unused]] std::string_view key, [[maybe_unused]] std::string_view value, [[maybe_unused]] write_version_type write_version, [[maybe_unused]] const std::vector<blob_id_type>& large_objects) {
    TRACE_START << "storage_id=" << storage_id << ", key=" << key << ",value = " << value << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_ << ", large_objects.size()=" << large_objects.size();
    if (large_objects.empty()) {
        add_entry(storage_id, key, value, write_version);
        return;
    }
    try {
        log_entry::write_with_blob(strm_, storage_id, key, value, write_version, large_objects);
        envelope_.add_persistent_blob_ids(large_objects);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_normal_with_blob(storage_id, key, value, write_version, large_objects);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
};

void log_channel::remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version) {
    TRACE_START << "storage_id=" << storage_id << ", key=" << key << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    try {
        log_entry::write_remove(strm_, storage_id, key, write_version);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_remove_entry(storage_id, key, write_version);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
}

void log_channel::add_storage(storage_id_type storage_id, write_version_type write_version) {
    TRACE_START << "storage_id=" << storage_id << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    try {
        log_entry::write_add_storage(strm_, storage_id, write_version);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_add_storage(storage_id, write_version);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    VLOG(log_trace_fine) << "end add_storage() with storage_id=" << storage_id << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    TRACE_END;
}

void log_channel::remove_storage(storage_id_type storage_id, write_version_type write_version) {
    TRACE_START << "storage_id=" << storage_id << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    try {
        log_entry::write_remove_storage(strm_, storage_id, write_version);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_remove_storage(storage_id, write_version);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
}

void log_channel::truncate_storage(storage_id_type storage_id, write_version_type write_version) {
    TRACE_START << "storage_id=" << storage_id << ", epoch =" << write_version.epoch_number_ << ", minor =" << write_version.minor_write_version_;
    try {
        log_entry::write_clear_storage(strm_, storage_id, write_version);
        impl_->send_replica_message(current_epoch_id_.load(), [&](replication::message_log_entries &msg) {
            msg.add_clear_storage(storage_id, write_version);
        });
    } catch (...) {
        TRACE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_END;
}

boost::filesystem::path log_channel::file_path() const noexcept {
    return location_ / file_;
}

// DO rotate without condition check.
//  use this after your check
std::string log_channel::do_rotate_file(epoch_id_type epoch) {
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

    return new_name;
}





log_channel_impl* log_channel::get_impl() const noexcept {
    return impl_.get();
}
} // namespace limestone::api
