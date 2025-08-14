#pragma once

#include <vector>
#include "wal_sync/wal_history.h"

namespace limestone::grpc::backend {

class backend_shared_impl {
public:
    explicit backend_shared_impl(const boost::filesystem::path& log_dir);
    virtual ~backend_shared_impl() = default;
    backend_shared_impl(const backend_shared_impl&) = delete;
    backend_shared_impl& operator=(const backend_shared_impl&) = delete;
    backend_shared_impl(backend_shared_impl&&) = delete;
    backend_shared_impl& operator=(backend_shared_impl&&) = delete;

    // Shared logic for listing WAL history
    std::vector<limestone::internal::wal_history::record> list_wal_history();

private:
    boost::filesystem::path log_dir_;
};

} // namespace limestone::grpc::backend
