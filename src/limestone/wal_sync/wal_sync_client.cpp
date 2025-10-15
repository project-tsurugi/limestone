#include <wal_sync/wal_sync_client.h>
#include <boost/filesystem.hpp>
#include <grpc/client/backup_client.h>
#include "file_operations.h"
#include "manifest.h"
#include "dblog_scan.h"
#include "remote_exception.h"
#include "wal_history.grpc.pb.h"
#include "grpc/service/message_versions.h"
#include "grpc/service/grpc_constants.h"
#include "wal_history.h"
namespace limestone::internal {

using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::list_wal_history_message_version;
using limestone::grpc::service::grpc_timeout_ms;

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


wal_sync_client::wal_sync_client(boost::filesystem::path log_dir, std::shared_ptr<::grpc::Channel> const& channel) noexcept
    : log_dir_(std::move(log_dir))
    , real_file_ops_()
    , file_ops_(&real_file_ops_)
    , history_client_(std::make_shared<limestone::grpc::client::wal_history_client>(channel))
    , backup_client_(std::make_shared<limestone::grpc::client::backup_client>(channel)) {}





epoch_id_type wal_sync_client::get_remote_epoch() {
    WalHistoryResponse response;
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    ::grpc::Status status = history_client_->get_wal_history(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        throw remote_exception(status, "WalHistoryService/GetWalHistory");
    }
    return response.last_epoch();
}

epoch_id_type wal_sync_client::get_local_epoch() {
    dblog_scan scan(log_dir_);
    return scan.last_durable_epoch_in_dir();
}

std::vector<branch_epoch> wal_sync_client::get_remote_wal_compatibility() {
    WalHistoryResponse response;
    WalHistoryRequest request;
    request.set_version(list_wal_history_message_version);
    ::grpc::Status status = history_client_->get_wal_history(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        throw remote_exception(status, "WalHistoryService/GetWalHistory");
    }
    std::vector<branch_epoch> result;
    result.reserve(response.records_size());
    for (const auto& record : response.records()) {
        result.emplace_back(branch_epoch{
            record.epoch(),
            record.identity(),
            record.timestamp()
        });
    }
    return result;
}

std::vector<branch_epoch> wal_sync_client::get_local_wal_compatibility() {
    wal_history wal_history_(log_dir_);
    auto records = wal_history_.list();
    std::vector<branch_epoch> result;
    result.reserve(records.size());
    for (const auto& record : records) {
        result.emplace_back(branch_epoch{
            record.epoch,
            record.identity,
            record.timestamp
        });
    }
    return result;
}

bool wal_sync_client::check_wal_compatibility(
    std::vector<branch_epoch> const& local,
    std::vector<branch_epoch> const& remote
) {
    if (local.empty() || remote.empty() || local.size() > remote.size()) {
        return false;
    }

    for (std::size_t i = 0; i < local.size(); ++i) {
        if (local[i].epoch != remote[i].epoch ||
            local[i].identity != remote[i].identity ||
            local[i].timestamp != remote[i].timestamp) {
            return false;
        }
    }
    return true;
}

begin_backup_result wal_sync_client::begin_backup(
    std::uint64_t begin_epoch,
    std::uint64_t end_epoch
) {
    BeginBackupRequest request;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(begin_epoch);
    request.set_end_epoch(end_epoch);

    BeginBackupResponse response;
    ::grpc::Status status = backup_client_->begin_backup(request, response, grpc_timeout_ms);
    if (!status.ok()) {
        throw remote_exception(status, "BackupService/BeginBackup");
    }

    begin_backup_result result{};
    result.session_token = response.session_id();
    result.expire_at = std::chrono::system_clock::time_point{std::chrono::seconds{response.expire_at()}};
    result.objects.reserve(static_cast<std::size_t>(response.objects_size()));
    for (auto const& object : response.objects()) {
        result.objects.emplace_back(backup_object{
            object.object_id(),
            static_cast<backup_object_type>(object.type()),
            object.path()
        });
    }
    return result;
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
