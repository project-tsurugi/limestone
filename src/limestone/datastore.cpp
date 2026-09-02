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
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <stdexcept>
#include <future>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <map>

#include <boost/filesystem/fstream.hpp>

#include <glog/logging.h>
#include <limestone/logging.h>
#ifdef ENABLE_ALTIMETER
#include <altimeter/event/constants.h>
#include <altimeter/log_item.h>
#include <altimeter/logger.h>
#endif
#include "logging_helper.h"
#include "limestone_exception_helper.h"

#include <limestone/api/datastore.h>
#include "internal.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"
#include "compaction_options.h"
#include "blob_file_resolver.h"
#include "blob_pool_impl.h"
#include "blob_file_garbage_collector.h"
#include "blob_file_gc_snapshot.h"
#include "blob_file_scanner.h"
#include "datastore_impl.h"
#include "manifest.h"
#include "log_channel_impl.h"
#include "dblog_scan.h"

namespace {

using namespace limestone;
using namespace limestone::api;

enum class file_write_mode {
    append,
    overwrite
};

void write_epoch_to_file_internal(const std::string& file_path, epoch_id_type epoch_id, file_write_mode mode) {
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

} // namespace


namespace limestone::api {
using namespace limestone::internal;

datastore::datastore() noexcept: impl_(std::make_unique<datastore_impl>()) {
    impl_->set_pid(::getpid());
}


datastore::datastore(configuration const& conf) : location_(conf.data_location_), impl_(std::make_unique<datastore_impl>()) { // NOLINT(readability-function-cognitive-complexity)
    try {
        impl_->set_instance_id(conf.instance_id_);
        impl_->set_db_name(conf.db_name_);
        impl_->set_pid(::getpid());
        auto const& repl_result = impl_->get_replication_config_result();
        if (!repl_result.ok) {
            std::string err_msg = "invalid replication configuration: " + repl_result.error_message;
            LOG_LP(ERROR) << err_msg;
            throw limestone_exception(exception_type::initialization_failure, err_msg);
        }
        if (repl_result.config.mode() == replication::replication_mode::rdma
            && !impl_->rdma_slot_count().has_value()) {
            std::string err_msg =
                "REPLICATION_RDMA_SLOTS must be set to a valid slot count in RDMA replication mode";
            LOG_LP(ERROR) << err_msg;
            throw limestone_exception(exception_type::initialization_failure, err_msg);
        }
        // The hybrid configuration (TCP control channel with RDMA data channels)
        // is not provided as a product, so REPLICATION_RDMA_SLOTS set in TCP mode
        // is rejected at startup as a misconfiguration (pure TCP or pure RDMA only).
        // The check uses the presence of the env var, not the parsed value: an
        // invalid value ("0", "abc", ...) is just as much a misconfiguration and
        // must not silently start as pure TCP.
        if (repl_result.config.mode() == replication::replication_mode::tcp
            && std::getenv("REPLICATION_RDMA_SLOTS") != nullptr) {
            std::string err_msg =
                "REPLICATION_RDMA_SLOTS must not be set in TCP replication mode; "
                "the hybrid configuration (TCP control channel with RDMA data channels) is not supported";
            LOG_LP(ERROR) << err_msg;
            throw limestone_exception(exception_type::initialization_failure, err_msg);
        }
        LOG(INFO) << "/:limestone:config:datastore setting log location = " << location_.string();
        boost::system::error_code error;
        const bool result_check = boost::filesystem::exists(location_, error);
        boost::filesystem::path manifest_path = boost::filesystem::path(location_) / std::string(internal::manifest::file_name);
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

        // acquire lock for manifest file
        fd_for_flock_ = manifest::acquire_lock(location_);
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

        auto migration_info = internal::check_and_migrate_logdir_format(location_);
        impl_->set_migration_info(migration_info);

        add_file(compaction_catalog_path);
        compaction_catalog_ = std::make_unique<compaction_catalog>(compaction_catalog::from_catalog_file(location_));

        // Verify consistency between the compaction catalog and the WAL files.
        // A cursor opens the compacted file named by the catalog, and the compacted
        // file is excluded from the snapshot input by the same catalog record
        // (see snapshot_impl and assemble_snapshot_input_filenames). Therefore a
        // compacted file that exists on disk but is not registered in the catalog
        // is fed to the snapshot input as an ordinary WAL, and remove entries are
        // not written, resurrecting deleted records (see tsurugi-issues #1498).
        // Conversely, a file registered in the catalog but missing from disk cannot
        // be opened by a cursor. Fail fast on either instead of corrupting data
        // silently.
        {
            // The compacted file exists on disk but is not registered in the catalog.
            // NOTE: only the single well-known compacted file is checked here. If
            // multiple compacted files are ever registered, this check must be extended.
            boost::filesystem::path compacted_file_path = location_ / compaction_catalog::get_compacted_filename();
            // Use the error_code overload so that a genuine filesystem error (permission,
            // broken symlink, I/O error, ...) is funneled into a limestone_exception rather
            // than escaping as a boost::filesystem::filesystem_error. A non-existent file is
            // the normal case: boost::filesystem::exists reports it via error_code as ENOENT,
            // so that condition must be excluded and must not be treated as an error.
            boost::system::error_code exists_error;
            bool compacted_file_exists = boost::filesystem::exists(compacted_file_path, exists_error);
            if (exists_error && exists_error != boost::system::errc::no_such_file_or_directory) {
                std::string err_msg = "failed to check existence of the compacted file '"
                    + compacted_file_path.string() + "': " + exists_error.message();
                LOG(ERROR) << "/:limestone:config:datastore " << err_msg;
                throw limestone_exception(exception_type::initialization_failure, err_msg);
            }
            if (compacted_file_exists) {
                bool registered = false;
                for (auto const& info : compaction_catalog_->get_compacted_files()) {
                    if (info.get_file_name() == compaction_catalog::get_compacted_filename()) {
                        registered = true;
                        break;
                    }
                }
                if (!registered) {
                    std::string err_msg = "compaction catalog is inconsistent: the compacted file '"
                        + compaction_catalog::get_compacted_filename()
                        + "' exists but is not registered in the catalog, log directory: " + location_.string();
                    LOG(ERROR) << "/:limestone:config:datastore " << err_msg;
                    throw limestone_exception(exception_type::initialization_failure, err_msg);
                }
            }

            // At most one compacted file may be registered.
            // get_current_compacted_file_name() relies on this check and returns the
            // first record.
            if (compaction_catalog_->get_compacted_files().size() > 1) {
                std::string err_msg = "compaction catalog is inconsistent: multiple compacted files are recorded, log directory: "
                    + location_.string();
                LOG(ERROR) << "/:limestone:config:datastore " << err_msg;
                throw limestone_exception(exception_type::initialization_failure, err_msg);
            }

            // Every compacted file registered in the catalog must exist on disk.
            // A cursor opens the compacted file named by the catalog rather than
            // falling back implicitly on its presence, so a missing file must be
            // rejected here. The carry file recorded by the catalog is verified to
            // exist in the same way: it is part of the snapshot input at startup, so a
            // missing carry would silently lose the snippets beyond the boundary.
            std::vector<std::string> recorded_files;
            for (auto const& info : compaction_catalog_->get_compacted_files()) {
                recorded_files.push_back(info.get_file_name());
            }
            const std::optional<std::string>& carry_file = compaction_catalog_->get_carry_file();
            if (carry_file.has_value()) {
                recorded_files.push_back(carry_file.value());
            }
            for (auto const& recorded_file : recorded_files) {
                boost::filesystem::path recorded_path = location_ / recorded_file;
                boost::system::error_code recorded_exists_error;
                bool recorded_exists = boost::filesystem::exists(recorded_path, recorded_exists_error);
                if (recorded_exists_error && recorded_exists_error != boost::system::errc::no_such_file_or_directory) {
                    std::string err_msg = "failed to check existence of the compacted file '"
                        + recorded_path.string() + "': " + recorded_exists_error.message();
                    LOG(ERROR) << "/:limestone:config:datastore " << err_msg;
                    throw limestone_exception(exception_type::initialization_failure, err_msg);
                }
                if (!recorded_exists) {
                    std::string err_msg = "compaction catalog is inconsistent: the file '"
                        + recorded_file
                        + "' is registered in the catalog but does not exist, log directory: " + location_.string();
                    LOG(ERROR) << "/:limestone:config:datastore " << err_msg;
                    throw limestone_exception(exception_type::initialization_failure, err_msg);
                }
            }
        }

        impl_->set_current_compacted_file_name(compaction_catalog_->get_current_compacted_file_name());

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

        impl_->initialize_blob_file_resolver(location_);
        auto blob_root = impl_->blob_file_resolver().get_blob_root();
        const bool blob_root_exists = boost::filesystem::exists(blob_root, error);
        if (!blob_root_exists || error) {
            const bool result_mkdir = boost::filesystem::create_directories(blob_root, error);
            if (!result_mkdir || error) {
                LOG_AND_THROW_IO_EXCEPTION("fail to create directory: " + blob_root.string(), error);
            }
        }
        VLOG_LP(log_debug) << "datastore is created, location = " << location_.string();
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

datastore::~datastore() noexcept{
    if (state_ == state::shutdown) {
        return;
    }
    try {
        shutdown();
    } catch (const std::exception &e) {
        LOG_LP(ERROR) << "Exception in destructor during shutdown: " << e.what();
    } catch (...) {
        LOG_LP(ERROR) << "Unknown exception in destructor during shutdown.";
    }
}


void datastore::recover() const noexcept {
    check_before_ready(static_cast<const char*>(__func__));
}



void datastore::persist_epoch_id(epoch_id_type epoch_id) {
    TRACE_START << "epoch_id=" << epoch_id;
    try {
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
#ifdef ENABLE_ALTIMETER
        if (::altimeter::logger::is_log_on(::altimeter::event::category,
                                           ::altimeter::event::level::log_data_store)) {
            ::altimeter::log_item log_item;
            log_item.category(::altimeter::event::category);
            log_item.type(::altimeter::event::type::wal_stored);
            log_item.level(::altimeter::event::level::log_data_store);
            log_item.add(::altimeter::event::item::instance_id, impl_->instance_id());
            log_item.add(::altimeter::event::item::dbname, impl_->db_name());
            log_item.add(::altimeter::event::item::pid, static_cast<std::int64_t>(impl_->pid()));
            std::string wal_version = std::to_string(epoch_id);
            log_item.add(::altimeter::event::item::wal_version, wal_version);
            log_item.add(::altimeter::event::item::result, ::altimeter::event::result::success);
            ::altimeter::logger::log(log_item);
        }
#endif
    } catch (...) {
#ifdef ENABLE_ALTIMETER
        if (::altimeter::logger::is_log_on(::altimeter::event::category,
                                           ::altimeter::event::level::log_data_store)) {
            ::altimeter::log_item log_item;
            log_item.category(::altimeter::event::category);
            log_item.type(::altimeter::event::type::wal_stored);
            log_item.level(::altimeter::event::level::log_data_store);
            log_item.add(::altimeter::event::item::instance_id, impl_->instance_id());
            log_item.add(::altimeter::event::item::dbname, impl_->db_name());
            log_item.add(::altimeter::event::item::pid, static_cast<std::int64_t>(impl_->pid()));
            std::string wal_version = std::to_string(epoch_id);
            log_item.add(::altimeter::event::item::wal_version, wal_version);
            log_item.add(::altimeter::event::item::result, ::altimeter::event::result::failure);
            ::altimeter::logger::log(log_item);
        }
#endif
        throw;
    }
    TRACE_END;
}

void datastore::log_wal_started(epoch_id_type wal_version, bool success) const {
#ifdef ENABLE_ALTIMETER
    if (!::altimeter::logger::is_log_on(::altimeter::event::category,
                                        ::altimeter::event::level::log_data_store)) {
        return;
    }
    ::altimeter::log_item log_item;
    log_item.category(::altimeter::event::category);
    log_item.type(::altimeter::event::type::wal_started);
    log_item.level(::altimeter::event::level::log_data_store);
    log_item.add(::altimeter::event::item::instance_id, impl_->instance_id());
    log_item.add(::altimeter::event::item::dbname, impl_->db_name());
    log_item.add(::altimeter::event::item::pid, static_cast<std::int64_t>(impl_->pid()));
    std::string wal_version_str = std::to_string(wal_version);
    log_item.add(::altimeter::event::item::wal_version, wal_version_str);
    log_item.add(::altimeter::event::item::result,
                 success ? ::altimeter::event::result::success : ::altimeter::event::result::failure);
    ::altimeter::logger::log(log_item);
#else
    (void)wal_version;
    (void)success;
#endif
}

blob_id_type datastore::create_snapshot_and_get_max_blob_id_with_wal_started_log() {
    try {
        auto max_blob_id = create_snapshot_and_get_max_blob_id();
        log_wal_started(static_cast<epoch_id_type>(epoch_id_informed_.load()), true);
        return max_blob_id;
    } catch (...) {
        // NOTE: This path may not appear in coverage because death tests run in a separate process,
        // and their coverage data is not merged into the parent process report.
        log_wal_started(static_cast<epoch_id_type>(epoch_id_informed_.load()), false);
        throw;
    }
}

void datastore::persist_and_propagate_epoch_id(epoch_id_type epoch_id) {
    TRACE_START << "epoch_id=" << epoch_id;
    if (impl_->is_async_group_commit_enabled()) {
        bool sent = impl_->propagate_group_commit(epoch_id);
        persist_epoch_id(epoch_id);
        if (sent) {
            impl_->wait_for_propagated_group_commit_ack();
        }
    } else {
        persist_epoch_id(epoch_id);
        bool sent = impl_->propagate_group_commit(epoch_id);
        if (sent) {
            impl_->wait_for_propagated_group_commit_ack();
        }
    }
    TRACE_END;
}

blob_reference_tag_type datastore::generate_reference_tag(
        blob_id_type blob_id,
        std::uint64_t transaction_id) {
    return impl_->generate_reference_tag(blob_id, transaction_id);
}

void datastore::ready() {
    TRACE_START;
    try {
        blob_id_type max_blob_id =
            std::max(create_snapshot_and_get_max_blob_id_with_wal_started_log(), compaction_catalog_->get_max_blob_id());
        blob_file_garbage_collector_ = std::make_unique<blob_file_garbage_collector>(impl_->blob_file_resolver());
        blob_file_garbage_collector_->scan_blob_files(max_blob_id);

        // Build the compacted path from the catalog record. With no record, pass the
        // well-known name that the migration rule assigns to generation 0; the startup
        // consistency check guarantees that no file by that name exists in that case.
        std::optional<std::string> compacted_file_name = impl_->get_current_compacted_file_name();
        boost::filesystem::path compacted_file =
            location_ / compacted_file_name.value_or(compaction_catalog::get_compacted_filename());
        boost::filesystem::path snapshot_file = location_ / std::string(snapshot::subdirectory_name_) / std::string(snapshot::file_name_);
        blob_file_garbage_collector_->scan_snapshot(snapshot_file, compacted_file);

        next_blob_id_.store(max_blob_id + 1);

        online_compaction_worker_future_ = std::async(std::launch::async, &datastore::online_compaction_worker, this);
        if (epoch_id_switched_.load() != 0) {
            write_epoch_callback_(epoch_id_informed_.load());
        }
        cleanup_rotated_epoch_files(location_);
        auto migration_info = impl_->get_migration_info();
        if (migration_info.has_value() && migration_info->requires_rotation()) {
            LOG(INFO) << "Manifest migration requires WAL rotation.";
            dblog_scan ds(location_);
            ds.detach_wal_files();
            LOG(INFO) << "WAL rotation completed.";
        }

        state_ = state::ready;
        if (impl_ ->is_replication_configured() && impl_->is_master()) {
            if (impl_->get_replication_config_result().config.mode() == replication_mode::rdma) {
                // RDMA mode: exchange DMA addresses via the handshake daemon; no TCP
                // connection is made.
                if (! impl_->establish_rdma_session()) {
                    LOG_LP(FATAL) << "Failed to establish the RDMA replication session.";
                }
                LOG_LP(INFO) << "Replication RDMA session established successfully.";
            } else {
                if (! impl_->establish_tcp_control_channel()) {
                    LOG_LP(FATAL) << "Failed to establish the replication control channel.";
                }
            }
        }
        TRACE_END;
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
    }
}

std::unique_ptr<snapshot> datastore::get_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::unique_ptr<snapshot>(new snapshot(location_, clear_storage, impl_->get_current_compacted_file_name()));
}

std::shared_ptr<snapshot> datastore::shared_snapshot() const {
    check_after_ready(static_cast<const char*>(__func__));
    return std::shared_ptr<snapshot>(new snapshot(location_, clear_storage, impl_->get_current_compacted_file_name()));
}

log_channel& datastore::create_channel() {
    TRACE_START;
    check_before_ready(static_cast<const char*>(__func__));

    std::uint64_t id{};
    auto& channel = impl_->register_log_channel([this, &id](std::uint64_t assigned_id) {
        id = assigned_id;
        // The log_channel constructor is private; construction stays here because
        // datastore is its only friend.
        return std::unique_ptr<log_channel>(new log_channel(location_, assigned_id, *this));
    });

    // In RDMA mode the per-channel TCP socket is unnecessary: log_channel
    // registration on the replica side is done by the TCP-less establishment
    // (establish_rdma_session), and replication data and ACKs both flow over
    // RDMA. Skip opening a connector to avoid leaving an idle TCP session per
    // log channel.
    if (impl_->has_replica() && impl_->is_master() && ! impl_->is_rdma_enabled()) {
        auto connector = impl_->create_log_channel_connector(*this, id);
        if (connector) {
            channel.get_impl()->set_replica_connector(std::move(connector));
        } else {
            LOG_LP(FATAL) << "Failed to create log channel connector.";
        }
    }
    TRACE_END << "id=" << id;
    return channel;
}

epoch_id_type datastore::last_epoch() const noexcept { return static_cast<epoch_id_type>(epoch_id_informed_.load()); }

void datastore::switch_epoch(epoch_id_type new_epoch_id) {
    TRACE_FINE_START << "new_epoch_id=" << new_epoch_id;
    try {
        check_after_ready(static_cast<const char*>(__func__));
        auto neid = static_cast<std::uint64_t>(new_epoch_id);
        if (auto switched = epoch_id_switched_.load(); neid <= switched) {
            LOG_LP(WARNING) << "switch to epoch_id_type of " << neid << " (<=" << switched << ") is curious";
        }

        on_switch_epoch_epoch_id_switched_store();    // for testing
        epoch_id_switched_.store(neid);
        if (state_ != state::not_ready) {
            update_min_epoch_id(true);
        }
    } catch (...) {
        TRACE_FINE_ABORT;
        HANDLE_EXCEPTION_AND_ABORT();
    }
    TRACE_FINE_END;
}

void datastore::update_min_epoch_id(bool from_switch_epoch) {  // NOLINT(readability-function-cognitive-complexity)
    TRACE_FINE_START << "from_switch_epoch=" << from_switch_epoch;
    
    on_update_min_epoch_id_epoch_id_switched_load(); // for testing
    auto upper_limit = epoch_id_switched_.load();
    if (upper_limit == 0) {
        return; // If epoch_id_switched_ is zero, it means no epoch has been switched, so updating epoch_id_to_be_recorded_ and epoch_id_informed_ is unnecessary.
    }
    upper_limit--;

    epoch_id_type max_finished_epoch = 0;

    for (const auto& e : impl_->log_channels()) {
        on_update_min_epoch_id_current_epoch_id_load(); // for testing
        auto working_epoch = e->current_epoch_id_.load();
        on_update_min_epoch_id_finished_epoch_id_load(); // for testing
        auto finished_epoch = e->finished_epoch_id_.load();
        if (working_epoch > finished_epoch && working_epoch != UINT64_MAX) {
            upper_limit = std::min(upper_limit, working_epoch - 1);
        }
        if (max_finished_epoch < finished_epoch && finished_epoch <= upper_limit) {
            max_finished_epoch = finished_epoch;
        }
    }

    TRACE_FINE << "epoch_id_switched_ = " << epoch_id_switched_.load() << ", upper_limit = " << upper_limit << ", max_finished_epoch = " << max_finished_epoch;

    // update recorded_epoch_
    auto to_be_epoch = std::min(upper_limit, static_cast<std::uint64_t>(max_finished_epoch));

    TRACE_FINE << "update epoch file part start with to_be_epoch = " << to_be_epoch;
    on_update_min_epoch_id_epoch_id_to_be_recorded_load();  // for testing
    auto old_epoch_id = epoch_id_to_be_recorded_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        on_update_min_epoch_id_epoch_id_to_be_recorded_cas();  // for testing
        if (epoch_id_to_be_recorded_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            TRACE_FINE << "epoch_id_to_be_recorded_ updated to " << to_be_epoch;
            on_update_min_epoch_id_epoch_id_to_be_recorded_load();  // for testing
            std::lock_guard<std::mutex> lock(mtx_epoch_file_);
            if (to_be_epoch < epoch_id_to_be_recorded_.load()) {
                break;
            }           
            write_epoch_callback_(static_cast<epoch_id_type>(to_be_epoch));
            epoch_id_record_finished_.store(to_be_epoch);
            TRACE_FINE << "epoch_id_record_finished_ updated to " << to_be_epoch;
            break;
        }
    }
    on_update_min_epoch_id_epoch_id_record_finished_load();  // for testing
    if (to_be_epoch > epoch_id_record_finished_.load()) {
        TRACE_FINE << "skipping persistent callback part, to_be_epoch =  " << to_be_epoch << ", epoch_id_record_finished_ = " << epoch_id_record_finished_.load();
        TRACE_FINE_END;
        return;
    }

    // update informed_epoch_
    to_be_epoch = upper_limit;
    TRACE_FINE << "persistent callback part start with to_be_epoch =" << to_be_epoch;
    // In `informed_epoch_`, the update restriction based on the `from_switch_epoch` condition is intentionally omitted.
    // Due to the interface specifications of Shirakami, it is necessary to advance the epoch even if the log channel
    // is not updated. This behavior differs from `recorded_epoch_` and should be maintained as such.
    on_update_min_epoch_id_epoch_id_informed_load_1();  // for testing
    old_epoch_id = epoch_id_informed_.load();
    while (true) {
        if (old_epoch_id >= to_be_epoch) {
            break;
        }
        on_update_min_epoch_id_epoch_id_informed_cas();  // for testing
        if (epoch_id_informed_.compare_exchange_strong(old_epoch_id, to_be_epoch)) {
            TRACE_FINE << "epoch_id_informed_ updated to " << to_be_epoch;
            {
                on_update_min_epoch_id_epoch_id_informed_load_2();  // for testing
                std::lock_guard<std::mutex> lock(mtx_epoch_persistent_callback_);
                if (to_be_epoch < epoch_id_informed_.load()) {
                    break;
                }
                if (persistent_callback_) {
                    TRACE_FINE <<  "start calling persistent callback to " << to_be_epoch;
                    persistent_callback_(to_be_epoch);
                    TRACE_FINE <<  "end calling persistent callback to " << to_be_epoch;
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
    TRACE_FINE_END;
}


void datastore::add_persistent_callback(std::function<void(epoch_id_type)> callback) noexcept {
    check_before_ready(static_cast<const char*>(__func__));
    persistent_callback_ = std::move(callback);
}

void datastore::remove_persistent_callback() noexcept {
    check_after_ready(static_cast<const char*>(__func__));
    std::lock_guard<std::mutex> lock(mtx_epoch_persistent_callback_);
    persistent_callback_ = {};
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

    // shutdown replication control channel
    if (impl_->is_replication_configured() && impl_->is_master()) {
        auto control_channel = impl_->get_control_channel();
        if (control_channel) {
            control_channel->close_session();
        }
    }

    // shutdown log channels
    for (auto const& lc : impl_->log_channels()) {
        auto replica_connector = lc->get_impl()->get_replica_connector();
        if (replica_connector) {
            replica_connector->close_session();
        }
    }

    // The send streams must be released before shutdown_rdma_sender() below
    // destroys the sender that owns them.
    impl_->release_rdma_send_streams();
    impl_->shutdown_rdma_sender();
    impl_->shutdown_rdma_ack_receiver();

    if (blob_file_garbage_collector_) {
        blob_file_garbage_collector_->shutdown();
    }

    // Make an in-flight rotation request give up: once shirakami stops on
    // shutdown, neither epoch progression nor end_session arrives, so the
    // rotation waits can no longer be satisfied. Files not renamed yet simply
    // stay unrotated and are handled by a request after the next startup.
    {
        auto& rotation_state = impl_->get_rotation_state();
        rotation_state.shutdown_requested.store(true);
        {
            std::lock_guard<std::mutex> lock(informed_mutex);
            cv_epoch_informed.notify_all();
        }
        {
            std::lock_guard<std::mutex> lock(rotation_state.mutex);
            rotation_state.pending_cv.notify_all();
        }
    }

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

    VLOG(log_info) << "/:limestone:datastore:shutdown end";
    return std::async(std::launch::async, [] {
    });
}

// old interface
backup& datastore::begin_backup() {
    try {
        auto tmp_files = get_files();
        
        // Use blob_file_scanner to add blob files to the backup target
        blob_file_scanner scanner(&impl_->blob_file_resolver());
        for (const auto& blob_file : scanner) {
            tmp_files.insert(blob_file);
        }
        
        backup_ = std::unique_ptr<backup>(new backup(tmp_files, *impl_));
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
        for (const auto& lc : impl_->log_channels()) {
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

                        // Skip a file whose name matches the unrotated pwal naming rule: its
                        // log channel may still open and append to it, so it cannot be saved
                        // as an immutable backup entry. As a result, the backup may be
                        // incomplete.
                        if (internal::is_unrotated_pwal_name(filename)) {
                            boost::system::error_code error;
                            bool result = boost::filesystem::is_empty(ent, error);
                            if (!error && !result) {
                                LOG_LP(ERROR) << "skip the file with an unrotated pwal name: " << filename;
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
                    if (filename == internal::manifest::file_name) {
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
        // Add blob files to the backup target
        blob_file_scanner scanner(&impl_->blob_file_resolver());
        // Use the parent of the blob root as the base for computing the relative path.
        boost::filesystem::path backup_root = impl_->blob_file_resolver().get_blob_root().parent_path();
        for (const auto& src : scanner) {
            entries.emplace_back(src, src.filename(), false, false);
        }
        

        return std::unique_ptr<backup_detail>(new backup_detail(entries, epoch_id_switched_.load(), *impl_));
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

rotation_result datastore::rotate_log_files() {  // NOLINT(readability-function-cognitive-complexity)
    TRACE_START;
    std::lock_guard<std::mutex> lock(rotate_mutex); 
    TRACE << "start rotate_log_files() critical section";
    auto epoch_id = epoch_id_switched_.load();
    if (epoch_id == 0) {
        LOG_AND_THROW_EXCEPTION("rotation requires epoch_id > 0, but got epoch_id = 0");
    }
    TRACE << "epoch_id = " << epoch_id;
    auto& rotation_state = impl_->get_rotation_state();
    {
        on_rotate_log_files(); // for testing
        // Wait until epoch_id_informed_ is less than rotated_epoch_id to ensure safe rotation.
        std::unique_lock<std::mutex> ul(informed_mutex);
        while (epoch_id_informed_.load() < epoch_id && !rotation_state.shutdown_requested.load()) {
            cv_epoch_informed.wait(ul);
        }
    }
    // No rotation is performed in the stop-preparation state (after shirakami
    // stops, neither epoch progression nor end_session arrives, so the waits
    // can never be satisfied). See the comment on the give-up path at the end
    // of this function for how the exception is handled.
    if (rotation_state.shutdown_requested.load()) {
        LOG_AND_THROW_EXCEPTION("rotation request aborted: shutdown requested");
    }
    TRACE << "end waiting for epoch_id_informed_ to catch up";
    VLOG_LP(log_info) << "rotation request started: boundary epoch = " << epoch_id;
    rotation_result result(epoch_id);
    bool aborted_by_shutdown = false;
    try {
        // Rotation target selection: scan the directory and enumerate the pwal
        // files with unrotated names (walking the channels would miss orphan
        // files bound to no channel).
        std::vector<boost::filesystem::path> targets;
        for (auto const& entry : boost::filesystem::directory_iterator(location_)) {
            if (!boost::filesystem::is_directory(entry.path()) &&
                internal::is_unrotated_pwal_name(entry.path().filename().string())) {
                targets.emplace_back(entry.path());
            }
        }

        // Lookup table (file name -> channel). The channel set is immutable
        // after registration completes, so this can be built outside the mutex.
        std::map<std::string, log_channel*> channel_by_filename;
        for (auto const& lc : impl_->log_channels()) {
            channel_by_filename.emplace(lc->file_path().filename().string(), lc.get());
        }

        std::size_t waited_channels = 0;
        std::chrono::steady_clock::time_point wait_begin{};
        {
            std::unique_lock<std::mutex> ul(rotation_state.mutex);
            rotation_state.renamed_files.clear();
            for (auto const& target : targets) {
                log_channel* channel = nullptr;
                if (auto it = channel_by_filename.find(target.filename().string()); it != channel_by_filename.end()) {
                    channel = it->second;
                }
                if (channel == nullptr) {
                    // Orphan file: nobody has it open, so it can be renamed on the spot.
                    impl_->on_rotate_before_rename();  // for testing
                    boost::filesystem::path renamed = internal::rotate_pwal_file(target, 0);
                    add_file(renamed);
                    subtract_file(target);
                    result.add_rotated_file(renamed.filename().string());
                    VLOG_LP(log_info) << "rotated an orphan pwal file: " << target.filename().string()
                                      << " -> " << renamed.filename().string();
                } else if (!channel->get_impl()->is_session_active_locked()) {
                    // Inactive channel: rename on the spot.
                    impl_->on_rotate_before_rename();  // for testing
                    result.add_rotated_file(channel->do_rotate_file());
                } else {
                    // Active channel: delegate the rename to the channel itself and wait for it.
                    channel->get_impl()->set_rotation_requested_locked();
                    rotation_state.pending_channels.insert(channel);
                }
            }
            waited_channels = rotation_state.pending_channels.size();
            impl_->on_rotate_before_wait();  // for testing
            wait_begin = std::chrono::steady_clock::now();
            rotation_state.pending_cv.wait(ul, [&rotation_state] {
                return rotation_state.pending_channels.empty() || rotation_state.shutdown_requested.load();
            });
            if (!rotation_state.pending_channels.empty()) {
                // Give-up on shutdown: roll the requested statuses back. Files
                // renamed before the give-up stay valid as rotated files; files
                // not renamed yet stay unrotated and are handled by a request
                // after the next startup.
                for (log_channel* channel : rotation_state.pending_channels) {
                    channel->get_impl()->clear_rotation_requested_locked();
                }
                rotation_state.pending_channels.clear();
                rotation_state.renamed_files.clear();
                aborted_by_shutdown = true;
            } else {
                for (auto const& renamed : rotation_state.renamed_files) {
                    result.add_rotated_file(renamed);
                }
                rotation_state.renamed_files.clear();
            }
        }
        if (!aborted_by_shutdown) {
            auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - wait_begin).count();
            VLOG_LP(log_info) << "rotation request completed: boundary epoch = " << epoch_id
                              << ", waited channels = " << waited_channels
                              << ", wait time = " << wait_ms << " ms";
        }
    } catch (...) {
        // A failure of the directory scan or a rename is unrecoverable (FATAL +
        // process exit); never leave the process running with a failed rename.
        HANDLE_EXCEPTION_AND_ABORT();
        throw; // Unreachable, but required to satisfy the compiler
    }
    if (aborted_by_shutdown) {
        // A give-up on shutdown is not an unrecoverable error, so it is thrown
        // here, outside the catch above (which turns errors into FATAL), and
        // returned to the caller as an exception. The online compaction worker
        // catches it and leaves its loop, letting the shutdown complete. Via
        // begin_backup it becomes FATAL through the existing error handling (a
        // backup during shutdown is an anomaly).
        LOG_AND_THROW_EXCEPTION("rotation request aborted: shutdown requested while waiting for in-flight sessions");
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
        LOG_AND_THROW_IO_EXCEPTION("does not have write permission for the log_location directory, path: " + location_.string(), errno);
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
    pthread_setname_np(pthread_self(), "cmpctn_worker");
    LOG_LP(INFO) << "online compaction worker started...";

    boost::filesystem::path ctrl_dir = location_ / "ctrl";
    boost::filesystem::path start_file = ctrl_dir / "start_compaction";


    ensure_directory_exists(ctrl_dir);
    if (!boost::filesystem::exists(ctrl_dir)) {
        if (!boost::filesystem::create_directory(ctrl_dir)) {
            LOG_LP(ERROR) << "failed to create directory: " << ctrl_dir.string();
            return;
        } 
    }

    while (!stop_online_compaction_worker_.load()) {
        if (boost::filesystem::exists(start_file)) {
            if (!boost::filesystem::remove(start_file)) {
                LOG_LP(ERROR) << "failed to remove file: " << start_file.string();
                return;
            }
            // Do not hold the mutex during the compaction (including the
            // rotation waits); otherwise stop_online_compaction_worker() cannot
            // set the stop flag and shutdown() blocks behind the rotation wait.
            try {
                compact_with_online();
            } catch (const limestone_exception& e) {
                if (stop_online_compaction_worker_.load()) {
                    // A rotation given up by a normal shutdown is not an anomaly.
                    LOG_LP(INFO) << "online compaction aborted by shutdown: " << e.what();
                } else {
                    LOG_LP(ERROR) << "failed to compact with online: " << e.what();
                }
            }
        }
        {
            std::unique_lock<std::mutex> lock(mtx_online_compaction_worker_);
            cv_online_compaction_worker_.wait_for(lock, std::chrono::seconds(1), [this]() {
                return stop_online_compaction_worker_.load();
            });
        }
    }
}

void datastore::stop_online_compaction_worker() {
    {
        std::lock_guard<std::mutex> lock(mtx_online_compaction_worker_);
        stop_online_compaction_worker_.store(true);
    }
    cv_online_compaction_worker_.notify_all();
}

void datastore::compact_with_online() {  // NOLINT(readability-function-cognitive-complexity)
    TRACE_START;
    check_after_ready(static_cast<const char*>(__func__));

    // Mutual exclusion with a backup: if the removal of the old generation ran between
    // the enumeration and the copy of a backup, the copy of an enumerated file would
    // fail. Compaction is therefore not run during a backup (as blob GC is not).
    if (impl_->is_backup_in_progress()) {
        LOG_LP(INFO) << "online compaction skipped because a backup is in progress";
        TRACE_END << "return compact_with_online() without compaction (backup in progress)";
        return;
    }

    // get a copy of next_blob_id and boundary_version before rotation
    blob_id_type next_blob_id_copy = next_blob_id_.load(std::memory_order_acquire);
    write_version_type boundary_version_copy;
    {
        std::lock_guard<std::mutex> lock(boundary_mutex_);
        boundary_version_copy = available_boundary_version_;
    }

    // Blob GC at online compaction is disabled: its exemption list is built only from
    // the scan of the compaction inputs, so it would delete a live blob whose entry
    // has not appeared in those inputs yet (issue #144). Blob files are collected only
    // by the GC at startup, until the fundamental fix of #144 re-enables this.
    bool blob_file_gc_runnable = false;
    bool is_active = blob_file_garbage_collector_->is_active();
    VLOG_LP(log_info) << "boundary_version_copy.get_major(): " << boundary_version_copy.get_major()
                            << ", compaction_catalog_->get_max_epoch_id(): " << compaction_catalog_->get_max_epoch_id()
                            << ", blob_file_garbage_collector_->is_active(): " << is_active
                            << ", blob_file_gc_runnable: " << blob_file_gc_runnable;

    // rotate first
    rotation_result result = rotate_log_files();

    // select files for compaction
    std::set<std::string> detached_pwals = compaction_catalog_->get_detached_pwals();

    
    for (const auto& filename : detached_pwals) {
        VLOG_LP(log_debug) << "detached_pwals:" << filename;
    }


    std::set<std::string> need_compaction_filenames = select_files_for_compaction(result.get_rotation_end_files(), detached_pwals);
    // Skip compaction when the only input is the current compacted file named by the
    // catalog.
    std::optional<std::string> current_compacted_file_name = impl_->get_current_compacted_file_name();
    if (need_compaction_filenames.empty() ||
        (need_compaction_filenames.size() == 1 && current_compacted_file_name.has_value() &&
         need_compaction_filenames.find(current_compacted_file_name.value()) != need_compaction_filenames.end())) {
        VLOG_LP(log_debug) << "no files to compact";
        TRACE_END << "return compact_with_online() without compaction";
        return;
    }

    for (const auto& filename : need_compaction_filenames) {
        VLOG_LP(log_debug) << "need_compaction_filenames: " << filename;
    }

    // create a temporary directory for online compaction
    boost::filesystem::path compaction_temp_dir = location_ / compaction_catalog::get_compaction_temp_dirname();
    ensure_directory_exists(compaction_temp_dir);

    // Set the appropriate options based on whether blob file GC is executable.
    VLOG_LP(log_info) << "blob_file_gc_runnable: " << blob_file_gc_runnable;
    compaction_options options = [&]() -> compaction_options {
        if (blob_file_gc_runnable) {
            auto gc_snapshot = std::make_unique<blob_file_gc_snapshot>(boundary_version_copy);
            return compaction_options{location_, compaction_temp_dir, recover_max_parallelism_, need_compaction_filenames, std::move(gc_snapshot)};
        }
        return compaction_options{location_, compaction_temp_dir, recover_max_parallelism_, need_compaction_filenames};
    }();
    // The compaction boundary is the rotation boundary (the epoch of rotation_result).
    // The durable epoch cannot serve as the boundary here: the inputs are only the
    // rotated files, so "every entry at or below the boundary is in the inputs" would
    // not hold for it.
    options.set_boundary_epoch(result.get_epoch_id());

    // Determine the file names of the new generation. The generation number increases
    // monotonically across online and offline compactions.
    std::uint64_t new_generation = compaction_catalog_->get_generation() + 1;
    std::string compacted_file_name = compaction_catalog::get_compacted_filename_for_generation(new_generation);
    std::string carry_file_name = compaction_catalog::get_carry_filename_for_generation(new_generation);
    options.set_output_file_names(compacted_file_name, carry_file_name);

    // Keep the file names of the old generation; they are removed after the commit
    // (the old generation leaves by removal, not by becoming a detached pwal).
    std::optional<std::string> old_compacted_file_name = compaction_catalog_->get_current_compacted_file_name();
    std::optional<std::string> old_carry_file_name = compaction_catalog_->get_carry_file();

    // Write phase: write the compacted file (and, if there are snippets beyond the
    // boundary, the carry file) into the temporary directory. An I/O error (a full disk,
    // for instance) has no visible effect because nothing is published yet, so the
    // temporary files are removed and the retry is left to the next compaction request.
    // A parse error (corrupted input) is unrecoverable and terminates the process.
    compaction_output_result output{};
    try {
        output = create_compaction_output(options);
    } catch (const limestone_io_exception& e) {
        LOG_LP(ERROR) << "online compaction failed while writing the temporary files"
                      << " (will be retried on the next compaction request): " << e.what();
        for (const std::string& name : {compacted_file_name, carry_file_name}) {
            boost::system::error_code remove_error;
            boost::filesystem::remove(compaction_temp_dir / name, remove_error);
            if (remove_error) {
                LOG_LP(FATAL) << "failed to remove the temporary compaction file '"
                              << (compaction_temp_dir / name).string() << "': " << remove_error.message();
            }
        }
        TRACE_END << "return compact_with_online() after a temporary-file write failure";
        return;
    } catch (const limestone_exception& e) {
        LOG_LP(FATAL) << "online compaction failed: the input files are corrupted: " << e.what();
    } catch (const std::exception& e) {
        LOG_LP(FATAL) << "online compaction failed while writing the temporary files: " << e.what();
    }
    blob_id_type max_blob_id = output.max_blob_id;

    // Publish, commit, then remove the old generation. A failure from here on is
    // unrecoverable and terminates the process: continuing would leave a generation file
    // that the catalog does not record, and the retry would republish under the same
    // generation name. The recovery completes with the orphan removal at the next startup
    // and the restoration of the catalog from its backup.
    boost::filesystem::path compacted_file = location_ / compacted_file_name;
    try {
        // Publish: rename from the temporary directory into the log directory (the
        // generation-suffixed name is unique, so nothing is overwritten).
        safe_rename(compaction_temp_dir / compacted_file_name, compacted_file);
        if (output.carry_written) {
            safe_rename(compaction_temp_dir / carry_file_name, location_ / carry_file_name);
        }

        // get a set of all files in the location_ directory
        std::set<std::string> files_in_location = get_files_in_directory(location_);

        // check if detached_pwals exist in location_
        for (auto it = detached_pwals.begin(); it != detached_pwals.end();) {
            if (files_in_location.find(*it) == files_in_location.end()) {
                VLOG_LP(log_debug) << "File " << *it << " does not exist in the directory and will be removed from detached_pwals.";
                auto p = location_ / *it;
                subtract_file(p);
                it = detached_pwals.erase(it);  // Erase and move to the next iterator
            } else {
                ++it;  // Move to the next iterator
            }
        }

        // The compacted and carry files of the old generation have been selected as
        // inputs (so they are in detached_pwals), but they leave by removal rather than
        // as detached pwals, so they are taken out of the set.
        if (old_compacted_file_name.has_value()) {
            detached_pwals.erase(old_compacted_file_name.value());
        }
        if (old_carry_file_name.has_value()) {
            detached_pwals.erase(old_carry_file_name.value());
        }

        // Commit: the catalog update is the only atomic commit point.
        // update_catalog_file keeps max_blob_id monotonically non-decreasing, so the
        // high-water mark recorded by previous compactions is preserved even though
        // max_blob_id here reflects only the freshly compacted files.
        compacted_file_info compacted_file_info{compacted_file_name, 1};
        compaction_catalog_->update_catalog_file(result.get_epoch_id(), max_blob_id, new_generation,
                                                 {compacted_file_info},
                                                 output.carry_written ? std::optional<std::string>(carry_file_name)
                                                                      : std::nullopt,
                                                 detached_pwals);
        // Refresh the synchronized copy of the compacted file name right after the commit.
        impl_->set_current_compacted_file_name(compaction_catalog_->get_current_compacted_file_name());
        add_file(compacted_file);
        if (output.carry_written) {
            add_file(location_ / carry_file_name);
        }
        VLOG_LP(log_info) << "compaction generation " << new_generation
                          << " committed (carry: " << (output.carry_written ? "yes" : "no") << ")";

        // Removal of the old generation: the commit is already done, so continuing
        // after a failure would let the old compacted file be selected as an input of
        // the next compaction by the shape of its name, which could merge two baseline
        // generations.
        for (const std::optional<std::string>& old_name : {old_compacted_file_name, old_carry_file_name}) {
            if (!old_name.has_value()) {
                continue;
            }
            boost::filesystem::path old_path = location_ / old_name.value();
            boost::system::error_code remove_error;
            boost::filesystem::remove(old_path, remove_error);
            if (remove_error) {
                LOG_LP(FATAL) << "failed to remove the old-generation compaction file '"
                              << old_path.string() << "': " << remove_error.message();
            }
            subtract_file(old_path);
        }
    } catch (const std::exception& e) {
        LOG_LP(FATAL) << "online compaction failed at the publish/commit/delete phase: " << e.what();
    }

    LOG_LP(INFO) << "compaction finished";

    // blob files garbage collection
    VLOG_LP(log_info) << "options.is_gc_enabled(): " << options.is_gc_enabled() << ", impl_->is_backup_in_progress(): " << impl_->is_backup_in_progress();
    if (options.is_gc_enabled() && !impl_->is_backup_in_progress()) {
        LOG_LP(INFO) << "start blob files garbage collection";
        blob_file_garbage_collector_->scan_blob_files(next_blob_id_copy);
        log_entry_container log_entries = options.get_gc_snapshot().finalize_snapshot();
        blob_file_garbage_collector_->start_add_gc_exempt_blob_ids();
        for (const auto& entry : log_entries) {
            for (const auto& blob_id : entry.get_blob_ids()) {
                blob_file_garbage_collector_->add_gc_exempt_blob_id(blob_id);
            }
        }
        blob_file_garbage_collector_->finalize_add_gc_exempt_blob_ids();
        LOG_LP(INFO) << "blob files garbage collection finished";
    }

    TRACE_END;
}

std::unique_ptr<blob_pool> datastore::acquire_blob_pool() {
    TRACE_START;

    // Define a lambda function for generating unique blob IDs in a thread-safe manner.
    // This function uses a CAS (Compare-And-Swap) loop to ensure atomic updates to the ID.
    // If the maximum value for blob IDs is reached, the function returns the max value, signaling an overflow condition.
    auto id_generator = [this]() {
        while (true) {
            blob_id_type current = next_blob_id_.load(std::memory_order_acquire); // Load the current ID atomically.
            if (current == std::numeric_limits<blob_id_type>::max()) {
                LOG_LP(ERROR) << "Blob ID overflow detected.";
                return current; // Return max value to indicate overflow.
            }
            if (next_blob_id_.compare_exchange_weak(
                    current,
                    current + 1,
                    std::memory_order_acq_rel, // Ensure atomicity of the update with acquire-release semantics.
                    std::memory_order_acquire)) {
                return current; // Return the successfully updated ID.
            }
        }
    };

    // Create a blob_pool_impl instance by passing the ID generator lambda and blob_file_resolver.
    // This approach allows flexible configuration and dependency injection for the blob pool.
    auto pool = std::make_unique<limestone::internal::blob_pool_impl>(
            id_generator,
            impl_->blob_file_resolver(),
            *this);
    TRACE_END;
    return pool; // Return the constructed blob pool.
}

blob_file datastore::get_blob_file(blob_id_type reference) {
    TRACE_START << "reference=" << reference;
    check_after_ready(static_cast<const char*>(__func__));
    auto path = impl_->resolve_blob_path(reference);
    bool available = reference < next_blob_id_.load(std::memory_order_acquire);
    if (available) {
        try {
            available = boost::filesystem::exists(path);
        } catch (const boost::filesystem::filesystem_error& e) {
            LOG_LP(ERROR) << "Failed to check blob file existence: " << e.what();
            available = false;
        }
    }
    TRACE_END << "path=" << path.string() << ", available=" << available;
    return blob_file(path, available);
}

void datastore::switch_available_boundary_version(write_version_type version) {
    TRACE_FINE_START << "version=" << version.get_major() << "." << version.get_minor();
    {
        std::lock_guard<std::mutex> lock(boundary_mutex_);
        if (version < available_boundary_version_) {
            LOG_LP(ERROR) << "The new boundary version (" << version.get_major() << ", " 
            << version.get_minor() << ") is smaller than the current boundary version (" 
            << available_boundary_version_.get_major() << ", " 
            << available_boundary_version_.get_minor() << ")";
            return;
        }
    }
    available_boundary_version_ = version;
    TRACE_FINE_END;
}

void datastore::add_persistent_blob_ids(const std::vector<blob_id_type>& blob_ids) {
    std::lock_guard<std::mutex> lock(persistent_blob_ids_mutex_);
    for (const auto& blob_id : blob_ids) {
        persistent_blob_ids_.insert(blob_id);
    }
}


std::vector<blob_id_type> datastore::check_and_remove_persistent_blob_ids(const std::vector<blob_id_type>& blob_ids) {
    std::lock_guard<std::mutex> lock(persistent_blob_ids_mutex_);
    std::vector<blob_id_type> not_found_blob_ids;

    for (const auto& blob_id : blob_ids) {
        auto it = persistent_blob_ids_.find(blob_id);
        if (it != persistent_blob_ids_.end()) {
            persistent_blob_ids_.erase(it);
        } else {
            not_found_blob_ids.push_back(blob_id);
        }
    }

    return not_found_blob_ids;
}

void datastore::wait_for_blob_file_garbace_collector_for_tests() const noexcept {
    if (blob_file_garbage_collector_) {
        blob_file_garbage_collector_->wait_for_all_threads();
    }
}

std::vector<std::unique_ptr<log_channel>> const& datastore::log_channels_for_tests() const noexcept {
    return impl_->log_channels();
}

} // namespace limestone::api
