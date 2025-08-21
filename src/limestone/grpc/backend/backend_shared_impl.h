#pragma once

#include <vector>
#include <optional>
#include <google/protobuf/repeated_field.h>
#include "wal_sync/wal_history.h"
#include "wal_history.grpc.pb.h"
#include "backup.grpc.pb.h"

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

private:
    boost::filesystem::path log_dir_;
};

} // namespace limestone::grpc::backend
