#pragma once

#include <google/protobuf/repeated_field.h>

#include <optional>
#include <vector>

#include "backup.grpc.pb.h"
#include "session.h"
#include "session_store.h"
#include "wal_history.grpc.pb.h"
#include "wal_sync/wal_history.h"
namespace limestone::grpc::backend {

using limestone::grpc::proto::BranchEpoch;
using limestone::grpc::proto::BackupObject;


class backend_shared_impl {
public:
    explicit backend_shared_impl(const boost::filesystem::path& log_dir);
    virtual ~backend_shared_impl() = default;
    backend_shared_impl(const backend_shared_impl&) = delete;
    backend_shared_impl& operator=(const backend_shared_impl&) = delete;
    backend_shared_impl(backend_shared_impl&&) = delete;
    backend_shared_impl& operator=(backend_shared_impl&&) = delete;

    // Shared logic for listing WAL history (returns proto repeated field)
    google::protobuf::RepeatedPtrField<BranchEpoch> list_wal_history();

    // Shared logic for creating backup objects from file paths
    static std::optional<BackupObject> make_backup_object_from_path(const boost::filesystem::path& path);

    // Create and register a session via session_store, return the created session
    std::optional<session> create_and_register_session(int64_t timeout_seconds, session::on_remove_callback_type on_remove = nullptr);

    // Shared logic for keep_alive
    ::grpc::Status keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept;

    // Shared logic for end_backup
    ::grpc::Status end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept;

    // Getter for session_store_ (for testing)
    const session_store& get_session_store() const noexcept;

private:
    boost::filesystem::path log_dir_;
    session_store session_store_;
};

} // namespace limestone::grpc::backend
