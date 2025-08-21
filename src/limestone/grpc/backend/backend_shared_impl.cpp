#include "backend_shared_impl.h"
#include <google/protobuf/repeated_field.h>
#include <optional>
namespace limestone::grpc::backend {

std::optional<limestone::grpc::proto::BackupObject> backend_shared_impl::make_backup_object_from_path(const boost::filesystem::path& path) {
    std::string filename = path.filename().string();

    static const std::set<std::string> metadata_files = {"compaction_catalog", "limestone-manifest.json", "wal_history"};

    if (filename == "pwal_0000.compacted") {
        limestone::grpc::proto::BackupObject obj;
        obj.set_object_id(filename);
        obj.set_path(filename);
        obj.set_type(limestone::grpc::proto::BackupObjectType::SNAPSHOT);
        return obj;
    }
    if (filename.rfind("pwal_", 0) == 0) {
        limestone::grpc::proto::BackupObject obj;
        obj.set_object_id(filename);
        obj.set_path(filename);
        obj.set_type(limestone::grpc::proto::BackupObjectType::LOG);
        return obj;
    }
    if ((metadata_files.count(filename) > 0) || filename.rfind("epoch.", 0) == 0) {
        limestone::grpc::proto::BackupObject obj;
        obj.set_object_id(filename);
        obj.set_path(filename);
        obj.set_type(limestone::grpc::proto::BackupObjectType::METADATA);
        return obj;
    }
    return std::nullopt;
}

using limestone::internal::wal_history;    

backend_shared_impl::backend_shared_impl(const boost::filesystem::path& log_dir) // NOLINT(modernize-pass-by-value)
    : log_dir_(log_dir) {}

google::protobuf::RepeatedPtrField<BranchEpoch> backend_shared_impl::list_wal_history() {
    wal_history wal_history_(log_dir_);
    auto records = wal_history_.list();
    google::protobuf::RepeatedPtrField<BranchEpoch> result;
    for (const auto& rec : records) {
        auto* out = result.Add();
        out->set_epoch(rec.epoch);
        out->set_identity(rec.identity);
        out->set_timestamp(static_cast<int64_t>(rec.timestamp));
    }
    return result;
}

} // namespace limestone::grpc::backend
