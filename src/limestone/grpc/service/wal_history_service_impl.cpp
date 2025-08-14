
#include "wal_history_service_impl.h"
#include <glog/logging.h>
#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::grpc::service {

wal_history_service_impl::wal_history_service_impl(grpc_service_backend& backend)
    : backend_(backend) {}


::grpc::Status wal_history_service_impl::ListWalHistory(
    ::grpc::ServerContext* /* context */,
    const limestone::grpc::proto::ListWalHistoryRequest* /* request */,
    limestone::grpc::proto::ListWalHistoryResponse* response) {
    VLOG_LP(log_info) << "ListWalHistory called";
    try {
        auto records = backend_.list_wal_history();
        for (const auto& rec : records) {
            auto* out = response->add_records();
            out->set_epoch(rec.epoch);
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            out->set_unique_id(std::string(reinterpret_cast<const char*>(rec.unique_id.data()), rec.unique_id.size()));
            out->set_timestamp(static_cast<int64_t>(rec.timestamp));
        }
        return {::grpc::Status::OK}; 
    } catch (const std::exception& e) {
        VLOG_LP(log_info) << "ListWalHistory failed: " << e.what();
        std::string msg = e.what();
        return {::grpc::StatusCode::INTERNAL, msg};
    }
}

} // namespace limestone::grpc::service
