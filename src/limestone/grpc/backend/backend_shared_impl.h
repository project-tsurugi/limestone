#pragma once

#include <vector>
#include <google/protobuf/repeated_field.h>
#include "wal_sync/wal_history.h"
#include "wal_history.grpc.pb.h"

namespace limestone::grpc::backend {

using BranchEpoch = limestone::grpc::proto::BranchEpoch;


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

private:
    boost::filesystem::path log_dir_;
};

} // namespace limestone::grpc::backend
