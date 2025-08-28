#include "backend_shared_impl.h"


#include <google/protobuf/repeated_field.h>
#include <cerrno>
#include <cstring>
#include <memory>
#include <optional>
#include "file_operations.h"

#include "grpc/service/message_versions.h"

namespace limestone::grpc::backend {

using limestone::grpc::service::keep_alive_message_version;
using limestone::grpc::service::end_backup_message_version;
using limestone::grpc::service::session_timeout_seconds;
using limestone::grpc::service::get_object_message_version;

std::optional<backup_object> backend_shared_impl::make_backup_object_from_path(const boost::filesystem::path& path) {
    std::string filename = path.filename().string();

    static const std::set<std::string> metadata_files = {"compaction_catalog", "limestone-manifest.json", "wal_history"};

    if (filename == "pwal_0000.compacted") {
        return backup_object(filename, backup_object_type::snapshot, filename);
    }
    if (filename.rfind("pwal_", 0) == 0) {
        return backup_object(filename, backup_object_type::log, filename);
    }
    if ((metadata_files.count(filename) > 0) || filename.rfind("epoch.", 0) == 0) {
        return backup_object(filename, backup_object_type::metadata, filename);
    }
    return std::nullopt;
}

using limestone::internal::wal_history;    

backend_shared_impl::backend_shared_impl(const boost::filesystem::path& log_dir, std::size_t chunk_size)
    : log_dir_(log_dir), chunk_size_(chunk_size), 
      default_file_ops_(std::make_unique<limestone::internal::real_file_operations>()),
      file_ops_(default_file_ops_.get()) {}

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

std::optional<session> backend_shared_impl::create_and_register_session(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, session::on_remove_callback_type on_remove) {
    return session_store_.create_and_register(begin_epoch, end_epoch, timeout_seconds, std::move(on_remove));
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

session_store& backend_shared_impl::get_session_store() noexcept {
    return session_store_;
}

void backend_shared_impl::set_file_operations(limestone::internal::file_operations* file_ops) noexcept {
    file_ops_ = file_ops;
}


::grpc::Status backend_shared_impl::get_object(
    const limestone::grpc::proto::GetObjectRequest* request,
    ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept {
    // Extract fields from request
    uint64_t version = request->version();
    if (version != get_object_message_version) {
        return {::grpc::StatusCode::INVALID_ARGUMENT, "unsupported get_object request version"};
    }

    const std::string& session_id = request->session_id();
    auto session = session_store_.get_session(session_id);
    if (!session) {
    return {::grpc::StatusCode::NOT_FOUND, "session not found: " + session_id};
    }
    grpc_writer_adapter adapter(writer);
    for (const auto& obj_id : request->object_id()) {
        auto backup_object = session->find_backup_object(obj_id);
        if (!backup_object) {
            return {::grpc::StatusCode::NOT_FOUND, "backup object not found: " + obj_id};
        }
        auto send_status = send_backup_object_data(*backup_object, &adapter);
        if (!send_status.ok()) {
            return send_status;
        }
    }
    return {::grpc::StatusCode::OK, "get_object successful"};
}

::grpc::Status backend_shared_impl::send_backup_object_data(
    const backup_object& object,
    i_writer* writer,
    std::streamoff start_offset,
    std::optional<std::streamoff> end_offset
) {
    auto abs_path = log_dir_ / object.path();
    std::unique_ptr<std::ifstream> ifs = file_ops_->open_ifstream(abs_path.string(), std::ios::binary);
    if (!ifs || !file_ops_->is_open(*ifs)) {
        int saved_errno = errno;
        return backend_shared_impl::make_stream_error_status("failed to open file", abs_path, std::nullopt, saved_errno);
    }

    file_ops_->ifs_seekg(*ifs, 0, std::ios::end);
    if (file_ops_->ifs_fail(*ifs)) {
        int saved_errno = errno;
        return backend_shared_impl::make_stream_error_status("failed to seek to end of file", abs_path, std::nullopt, saved_errno);
    }
    std::streamsize total_size = file_ops_->ifs_tellg(*ifs);
    if (file_ops_->ifs_fail(*ifs) || total_size == -1) {
        int saved_errno = errno;
        return backend_shared_impl::make_stream_error_status("failed to get file size", abs_path, std::nullopt, saved_errno);
    }
    if (start_offset < 0 || start_offset > total_size) {
        return ::grpc::Status(::grpc::StatusCode::OUT_OF_RANGE, "start_offset out of range");
    }
    std::streamoff effective_end = end_offset ? std::min(*end_offset, total_size) : total_size;
    if (effective_end < start_offset) {
        return ::grpc::Status(::grpc::StatusCode::OUT_OF_RANGE, "end_offset before start_offset");
    }
    std::streamsize send_size = effective_end - start_offset;
    file_ops_->ifs_seekg(*ifs, start_offset, std::ios::beg);
    if (file_ops_->ifs_fail(*ifs)) {
        int saved_errno = errno;
        return backend_shared_impl::make_stream_error_status("failed to seek to start_offset", abs_path, start_offset, saved_errno);
    }

    std::vector<char> buffer(chunk_size_);
    std::streamsize offset = start_offset;
    bool is_first = true;
    std::streamsize remaining = send_size;
    while (remaining > 0) {
        std::streamsize to_read = std::min(static_cast<std::streamsize>(buffer.size()), remaining);
        file_ops_->ifs_read(*ifs, buffer.data(), to_read);
        if (file_ops_->ifs_bad(*ifs)) {
            int saved_errno = errno;
            return backend_shared_impl::make_stream_error_status("failed to read file chunk", abs_path, offset, saved_errno);
        }
        std::streamsize bytes_read = file_ops_->ifs_gcount(*ifs);
        if (bytes_read <= 0) {
            if (file_ops_->ifs_fail(*ifs) || !file_ops_->ifs_eof(*ifs)) {
                int saved_errno = errno;
                return backend_shared_impl::make_stream_error_status("failed to read file chunk", abs_path, offset, saved_errno);
            }
            break;
        }
        limestone::grpc::proto::GetObjectResponse resp;
        // Fill BackupObject info
        auto* obj = resp.mutable_object();
        obj->set_object_id(object.object_id());
        obj->set_type(static_cast<limestone::grpc::proto::BackupObjectType>(object.type()));
        obj->set_path(object.path().string());

        if (is_first) {
            resp.set_total_size(static_cast<int64_t>(total_size));
        }
        resp.set_offset(static_cast<int64_t>(offset));
        resp.set_chunk(buffer.data(), static_cast<size_t>(bytes_read));
        resp.set_is_first(is_first);
        resp.set_is_last(offset + bytes_read >= effective_end);

        if (!writer->Write(resp)) {
            return ::grpc::Status(::grpc::StatusCode::UNKNOWN, "stream write failed");
        }
        offset += bytes_read;
        remaining -= bytes_read;
        is_first = false;
    }
    if (offset < start_offset + send_size) {
        return ::grpc::Status(::grpc::StatusCode::DATA_LOSS, "file truncated during read: " + abs_path.string());
    }
    return ::grpc::Status::OK;
}

::grpc::Status backend_shared_impl::make_stream_error_status(const std::string& context, const boost::filesystem::path& path, std::optional<std::streamoff> offset, int err) {
    std::string err_msg = context + ": " + path.string();
    if (offset) {
        err_msg += ", offset=" + std::to_string(*offset);
    }
    err_msg += ", errno=" + std::to_string(err) + ", " + std::strerror(err);
    ::grpc::StatusCode code;
    switch (err) {
        case ENOENT:
            code = ::grpc::StatusCode::NOT_FOUND;
            break;
        case EACCES:
        case EPERM:
            code = ::grpc::StatusCode::PERMISSION_DENIED;
            break;
        default:
            code = ::grpc::StatusCode::INTERNAL;
            break;
    }
    return ::grpc::Status(code, err_msg);
}

void backend_shared_impl::reset_file_operations_to_default() noexcept {
    file_ops_ = default_file_ops_.get();
}

}  // namespace limestone::grpc::backend
