#include "backend_shared_impl.h"
namespace limestone::grpc::backend {

using limestone::internal::wal_history;    

backend_shared_impl::backend_shared_impl(const boost::filesystem::path& log_dir) // NOLINT(modernize-pass-by-value)
    : log_dir_(log_dir) {}

std::vector<wal_history::record> backend_shared_impl::list_wal_history() {
    wal_history wal_history_(log_dir_);
    return wal_history_.list();
}

} // namespace limestone::grpc::backend
