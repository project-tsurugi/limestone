#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>

#include <grpc/client/backup_client.h>
#include <grpc/client/wal_history_client.h>
#include <limestone/api/epoch_id_type.h>
#include <wal_sync/backup_object_type.h>

#include "file_operations.h"

namespace limestone::internal {

using limestone::api::epoch_id_type; 
using limestone::internal::backup_object_type;

using unix_timestamp_seconds = std::int64_t;

struct branch_epoch {
    epoch_id_type epoch;
    std::uint64_t identity;
    unix_timestamp_seconds timestamp;
};

struct backup_object {
    std::string id;
    backup_object_type type;
    std::string path;
};

/**
 * @brief Result of begin_backup operation.
 */
struct begin_backup_result {
    std::string session_token;
    std::chrono::system_clock::time_point expire_at;
    std::vector<backup_object> objects;
};

/**
 * @brief Replica WAL sync client 
 */
class wal_sync_client {

public:
    /**
     * @brief Construct wal_sync_client with log directory path and gRPC channel
     * @param log_dir log directory path
     * @param channel gRPC channel to use for wal_history_client
     */
    wal_sync_client(boost::filesystem::path log_dir, std::shared_ptr<::grpc::Channel> const& channel) noexcept;

    wal_sync_client(wal_sync_client const&) = delete;
    wal_sync_client& operator=(wal_sync_client const&) = delete;
    wal_sync_client(wal_sync_client&&) = delete;
    wal_sync_client& operator=(wal_sync_client&&) = delete;

    /**
     * @brief Destructor.
     */
    ~wal_sync_client();

    /**
     * @brief Get the epoch value from the remote backup service.
     * @return remote node's durable epoch value
     * @throw remote_exception if the remote call fails or the response is invalid
     */
    epoch_id_type get_remote_epoch();

    /**
     * @brief Get the epoch value of the local node.
     * @return local node's durable epoch value
     */
    epoch_id_type get_local_epoch();

    /**
     * @brief Get WAL compatibility info from the remote backup service.
     * @return WAL history/compatibility info
     */
    std::vector<branch_epoch> get_remote_wal_compatibility();

    /**
     * @brief Get WAL compatibility info of the local node.
     * @return WAL history/compatibility info
     */
    std::vector<branch_epoch> get_local_wal_compatibility();

    /**
     * @brief Compare local and remote WAL compatibility info.
     * @param local local WAL history
     * @param remote remote WAL history
     * @return true if compatible, false otherwise
     */
    bool check_wal_compatibility(
        std::vector<branch_epoch> const& local,
        std::vector<branch_epoch> const& remote
    );

    /**
     * @brief Start backup session and get list of backup objects.
     * @param begin_epoch start epoch (inclusive)
     * @param end_epoch end epoch (exclusive, 0 for latest)
     * @return result containing session token, expiration, and objects
     */
    begin_backup_result begin_backup(
        std::uint64_t begin_epoch,
        std::uint64_t end_epoch
    );

    /**
     * @brief Request copy of backup objects from remote.
     * @param session_token session token
     * @param objects list of objects to copy
     * @return true if copy succeeded
     */
    bool copy_backup_objects(
        std::string const& session_token,
        std::vector<backup_object> const& objects
    );

    /**
     * @brief Extend the session expiration.
     * @param session_token session token
     * @return true if extension succeeded
     */
    bool keepalive_session(std::string const& session_token);

    /**
     * @brief End the backup session.
     * @param session_token session token
     * @return true if session ended successfully
     */
    bool end_backup(std::string const& session_token);

    /**
     * @brief Deploy copied files to the local data directory.
     * @param objects list of objects to deploy
     * @return true if deployment succeeded
     */
    bool deploy_objects(std::vector<backup_object> const& objects);

    /**
     * @brief Merge/compact WAL files if needed after incremental backup.
     * @return true if compaction succeeded
     */
    bool compact_wal();

    /**
     * @brief Initialize the client and validate or initialize the log directory and manifest.
     *
     * - If log_dir_ does not exist:
     *     - allow_initialize=true: create_directory is called and manifest is initialized.
     *     - allow_initialize=false: returns error.
     * - If log_dir_ exists and is empty:
     *     - allow_initialize=true: manifest is initialized.
     *     - allow_initialize=false: returns error.
     * - If log_dir_ exists and is not empty:
     *     - manifest version etc. are validated.
     *
     * @param error_message Set to error reason on failure.
     * @param allow_initialize Whether to allow initialization if directory does not exist or is empty.
     * @return true: success, false: error (see error_message)
     */
    bool init(std::string& error_message, bool allow_initialize);

    /**
     * @brief Set custom file_operations implementation (for testing)
     * @param file_ops file_operations implementation to use
     */
    void set_file_operations(file_operations& file_ops);

private:
    boost::filesystem::path log_dir_;
    real_file_operations real_file_ops_;
    file_operations* file_ops_;
    int lock_fd_ = -1;
    std::shared_ptr<limestone::grpc::client::wal_history_client> history_client_;
    std::shared_ptr<limestone::grpc::client::backup_client> backup_client_;
};

} // namespace limestone::internal
