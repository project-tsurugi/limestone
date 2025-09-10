
#include <wal_sync/wal_sync_client.h>
#include <boost/filesystem.hpp>
#include "file_operations.h"
#include "manifest.h"
#include "dblog_scan.h"

namespace limestone::internal {
bool wal_sync_client::init(std::string& error_message, bool allow_initialize) {

    // Check if log_dir_ exists and is a directory
    if (!boost::filesystem::exists(log_dir_)) {
        if (allow_initialize) {
            boost::system::error_code ec;
            if (!boost::filesystem::create_directory(log_dir_, ec) || ec) {
                error_message = "failed to create log_dir: " + log_dir_.string() + ", " + ec.message();
                return false;
            }
        } else {
            error_message = "log_dir does not exist: " + log_dir_.string();
            return false;
        }
    } else if (!boost::filesystem::is_directory(log_dir_)) {
        error_message = "log_dir is not a directory: " + log_dir_.string();
        return false;
    } else if (boost::filesystem::directory_iterator(log_dir_) == boost::filesystem::directory_iterator()) {
        // Directory exists but is empty
        if (!allow_initialize) {
            error_message = "log_dir is empty: " + log_dir_.string();
            return false;
        }
    }

    // If directory was just created or is empty and allow_initialize, create manifest
    if (allow_initialize && (
            !boost::filesystem::exists(log_dir_)
            || boost::filesystem::directory_iterator(log_dir_) == boost::filesystem::directory_iterator())) {
        manifest::create_initial(log_dir_);
    }

    real_file_operations ops;
    boost::filesystem::path manifest_path = log_dir_ / std::string(manifest::file_name);
    auto manifest_opt = manifest::load_manifest_from_path(manifest_path, ops);
    if (!manifest_opt) {
        error_message = "manifest file not found or invalid: " + manifest_path.string();
        return false;
    }
    const std::string& format_version = manifest_opt->get_format_version();
    if (format_version != manifest::default_format_version) {
        error_message = "unsupported manifest format_version: '" + format_version +
            "' (expected: '" + std::string(manifest::default_format_version) + "')";
        return false;
    }
    int persistent_version = manifest_opt->get_persistent_format_version();
    if (persistent_version != manifest::default_persistent_format_version) {
        error_message = "unsupported manifest persistent_format_version: " + std::to_string(persistent_version) +
            " (expected: " + std::to_string(manifest::default_persistent_format_version) + ")";
        return false;
    }

    // Acquire manifest lock (will be released in destructor)
    lock_fd_ = manifest::acquire_lock(log_dir_, *file_ops_);
    if (lock_fd_ < 0) {
        error_message = "failed to acquire manifest lock: " + log_dir_.string();
        return false;
    }
    return true;
}

// destructor: release manifest lock if held
wal_sync_client::~wal_sync_client() {
    if (lock_fd_ >= 0 && file_ops_) {
        file_ops_->close(lock_fd_);
        lock_fd_ = -1;
    }
}

wal_sync_client::wal_sync_client(boost::filesystem::path const& log_dir) noexcept
    : log_dir_(log_dir)
    , real_file_ops_()
    , file_ops_(&real_file_ops_) {}



std::uint64_t wal_sync_client::get_remote_epoch() {
    // TODO: implement
    return 0;
}


std::uint64_t wal_sync_client::get_local_epoch() {
    dblog_scan scan(log_dir_);
    return scan.last_durable_epoch_in_dir();
}

std::vector<branch_epoch> wal_sync_client::get_remote_wal_compatibility() {
    // TODO: implement
    return {};
}

std::vector<branch_epoch> wal_sync_client::get_local_wal_compatibility() {
    // TODO: implement
    return {};
}

bool wal_sync_client::check_wal_compatibility(
    std::vector<branch_epoch> const& local,
    std::vector<branch_epoch> const& remote
) {
    (void)local;
    (void)remote;
    // TODO: implement
    return false;
}

std::vector<backup_object> wal_sync_client::begin_backup(
    std::uint64_t begin_epoch,
    std::uint64_t end_epoch,
    std::string& session_token,
    std::chrono::system_clock::time_point& expire_at
) {
    (void)begin_epoch;
    (void)end_epoch;
    (void)session_token;
    (void)expire_at;
    // TODO: implement
    return {};
}

bool wal_sync_client::copy_backup_objects(
    std::string const& session_token,
    std::vector<backup_object> const& objects
) {
    (void)session_token;
    (void)objects;
    // TODO: implement
    return false;
}

bool wal_sync_client::keepalive_session(std::string const& session_token) {
    (void)session_token;
    // TODO: implement
    return false;
}

bool wal_sync_client::end_backup(std::string const& session_token) {
    (void)session_token;
    // TODO: implement
    return false;
}

bool wal_sync_client::deploy_objects(std::vector<backup_object> const& objects) {
    (void)objects;
    // TODO: implement
    return false;
}

bool wal_sync_client::compact_wal() {
    // TODO: implement
    return false;
}

void wal_sync_client::set_file_operations(file_operations& file_ops) {
    file_ops_ = &file_ops;
}

} // namespace limestone::internal
