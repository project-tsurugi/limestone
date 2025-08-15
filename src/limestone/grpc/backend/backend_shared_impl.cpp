#include "backend_shared_impl.h"
#include <google/protobuf/repeated_field.h>
namespace limestone::grpc::backend {

using limestone::internal::wal_history;    

backend_shared_impl::backend_shared_impl(const boost::filesystem::path& log_dir) // NOLINT(modernize-pass-by-value)
    : log_dir_(log_dir) {}

google::protobuf::RepeatedPtrField<limestone::grpc::proto::BranchEpoch> backend_shared_impl::list_wal_history() {
    wal_history wal_history_(log_dir_);
    auto records = wal_history_.list();
    google::protobuf::RepeatedPtrField<limestone::grpc::proto::BranchEpoch> result;
    for (const auto& rec : records) {
        auto* out = result.Add();
        out->set_epoch(rec.epoch);
        out->set_identity(rec.identity);
        out->set_timestamp(static_cast<int64_t>(rec.timestamp));
    }
    return result;
}

} // namespace limestone::grpc::backend
