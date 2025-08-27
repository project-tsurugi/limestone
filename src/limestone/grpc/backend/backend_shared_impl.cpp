#include "backend_shared_impl.h"
#include <google/protobuf/repeated_field.h>
#include <optional>

#include "grpc/service/message_versions.h"
#include "grpc/service/grpc_constants.h"
namespace limestone::grpc::backend {

using limestone::grpc::service::keep_alive_message_version;
using limestone::grpc::service::end_backup_message_version;
using limestone::grpc::service::session_timeout_seconds;

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

std::optional<session> backend_shared_impl::create_and_register_session(int64_t timeout_seconds, session::on_remove_callback_type on_remove) {
    return session_store_.create_and_register(timeout_seconds, std::move(on_remove));
}

::grpc::Status backend_shared_impl::keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept {
    uint64_t version = request->version();
    if (version != keep_alive_message_version) {
        return {::grpc::StatusCode::INVALID_ARGUMENT, "unsupported keep_alive request version"};
    }
    const auto& session_id = request->session_id();
    auto session = session_store_.get_and_refresh(session_id, session_timeout_seconds);
    if (!session) {
        return {::grpc::StatusCode::NOT_FOUND, "session not found or expired"};
    }
    response->set_expire_at(session->expire_at());
    return {::grpc::StatusCode::OK, "keep_alive successful"};
}

::grpc::Status backend_shared_impl::end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* /*response*/) noexcept {
    uint64_t version = request->version();
    if (version != end_backup_message_version) {
        return {::grpc::StatusCode::INVALID_ARGUMENT, "unsupported end_backup request version"};
    }
    const auto& session_id = request->session_id();
    session_store_.remove_session(session_id);

    return {::grpc::StatusCode::OK, "end_backup successful"};
}



} // namespace limestone::grpc::backend
