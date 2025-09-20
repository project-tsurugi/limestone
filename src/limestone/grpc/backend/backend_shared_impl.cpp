#include "backend_shared_impl.h"

#include <google/protobuf/repeated_field.h>

#include <cerrno>
#include <cstring>
#include <memory>
#include <optional>

#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "file_operations.h"
#include "grpc/service/message_versions.h"
#include "limestone/api/backup_detail.h"
#include "log_entry.h"

namespace limestone::grpc::backend {

using limestone::api::log_entry;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::end_backup_message_version;
using limestone::grpc::service::get_object_message_version;
using limestone::grpc::service::keep_alive_message_version;
using limestone::grpc::service::session_timeout_seconds;
using limestone::internal::compaction_catalog;
using limestone::internal::wal_history;

std::vector<backup_object> backend_shared_impl::generate_backup_objects(const std::vector<boost::filesystem::path>& paths, bool is_full_backup) {
    std::vector<backup_object> backup_objects;

    for (const auto& path : paths) {
        std::string filename = path.filename().string();

        static const std::set<std::string> metadata_files = {"compaction_catalog", "limestone-manifest.json", "wal_history"};

        if (filename == "pwal_0000.compacted") {
            if (!is_full_backup) {
                continue;  // Skip the snapshot file in incremental backup mode
            }
            backup_objects.emplace_back(filename, backup_object_type::snapshot, filename);
        } else if (filename.rfind("pwal_", 0) == 0) {
            backup_objects.emplace_back(filename, backup_object_type::log, filename);
        } else if ((metadata_files.count(filename) > 0)) {
            if (!is_full_backup && filename != "wal_history") {
                continue;  // Skip metadata files except wal_history in incremental backup mode
            }
            backup_objects.emplace_back(filename, backup_object_type::metadata, filename);
        } else if (filename.rfind("epoch", 0) == 0 && is_full_backup) {
            backup_objects.emplace_back(filename, backup_object_type::metadata, filename);
        }
    }
    return backup_objects;
}

backend_shared_impl::backend_shared_impl(boost::filesystem::path log_dir, std::size_t chunk_size)
    : log_dir_(std::move(log_dir)), chunk_size_(chunk_size), 
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
    i_writer* writer) noexcept {
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
    auto begin_epoch = session->begin_epoch();
    auto end_epoch = session->end_epoch();
    bool is_fullbackup = (begin_epoch == 0 && end_epoch == 0);
    std::set<blob_id_type> required_blobs;
    for (const auto& obj_id : request->object_id()) {
        auto backup_object = session->find_backup_object(obj_id);
        if (!backup_object) {
            return {::grpc::StatusCode::NOT_FOUND, "backup object not found: " + obj_id};
        }
        std::optional<byte_range> range = byte_range{0, std::nullopt};
        auto type = backup_object.value().type();
        if (type == backend::backup_object_type::log && !is_fullbackup) {
            ::grpc::Status error_status;
            range = prepare_log_object_copy(*backup_object, begin_epoch, end_epoch, required_blobs, error_status);
            if (!range) {
                return error_status;
            }
            if (range->end_offset.has_value() && range->end_offset == 0) {
                continue; // File does not have a copy target
            }
        }
        auto send_status = send_backup_object_data(*backup_object, writer, range.value());
        if (!send_status.ok()) {
            return send_status;
        }
    }
    return {::grpc::StatusCode::OK, "get_object successful"};
}


std::optional<byte_range> backend_shared_impl::prepare_log_object_copy(
    const backup_object& object,
    epoch_id_type begin_epoch,
    epoch_id_type end_epoch,
    std::set<blob_id_type>& required_blobs,
    ::grpc::Status& error_status
) {
    std::optional<std::streamoff> start_offset = std::nullopt;
    std::optional<std::streamoff> end_offset = std::nullopt;
    const auto& path = object.path();
    auto stream = file_ops_->open_ifstream((log_dir_ / path).string(), std::ios::binary);
    if (!stream || !file_ops_->is_open(*stream)) {
        int saved_errno = errno;
        error_status = backend_shared_impl::make_stream_error_status("failed to open file", path.filename(), std::nullopt, saved_errno);
        return std::nullopt;
    }
    log_entry entry;
    log_entry::read_error read_error_{};
    epoch_id_type current_epoch_id = 0;
    while (true) {
        auto fpos_before_read_entry = file_ops_->ifs_tellg(*stream);
        // Since read_entry_from does not distinguish between system call errors and format errors,
        // explicitly reset errno here to prevent inappropriate errno values from being included in error messages.
        errno = 0; 
        bool data_remains = entry.read_entry_from(*stream, read_error_);
        if (read_error_.value() != log_entry::read_error::ok) {
            std::string context = "file is corrupted: failed to read entry at fpos=" + std::to_string(static_cast<std::streamoff>(fpos_before_read_entry));
            error_status = backend_shared_impl::make_stream_error_status(context, path.filename(), fpos_before_read_entry, errno);
            return std::nullopt;
        }
        if (!data_remains) {
            break;
        }
        if (entry.type() == log_entry::entry_type::marker_begin) {
            current_epoch_id = entry.epoch_id();
            if (!start_offset.has_value() && entry.epoch_id() >= begin_epoch) {
                start_offset = fpos_before_read_entry;
            }
            if (!end_offset.has_value() && entry.epoch_id() >= end_epoch) {
                end_offset = fpos_before_read_entry;
            }
        }
        if (entry.type() == log_entry::entry_type::normal_with_blob && current_epoch_id >= begin_epoch && current_epoch_id < end_epoch) {
            auto blob_ids = entry.get_blob_ids();
            required_blobs.insert(blob_ids.begin(), blob_ids.end());
        }
    }
    if (start_offset.has_value()) {
        return {byte_range{start_offset.value(), end_offset}};
    }
    return {byte_range{0, 0}};
}

::grpc::Status backend_shared_impl::send_backup_object_data(
    const backup_object& object,
    i_writer* writer,
    const byte_range& range
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
    if (range.start_offset < 0 || range.start_offset > total_size) {
        return {::grpc::StatusCode::OUT_OF_RANGE, "start_offset out of range"};
    }
    std::streamoff effective_end = range.end_offset ? std::min(*range.end_offset, total_size) : total_size;
    if (effective_end < range.start_offset) {
        return {::grpc::StatusCode::OUT_OF_RANGE, "end_offset before start_offset"};
    }
    std::streamsize send_size = effective_end - range.start_offset;
    file_ops_->ifs_seekg(*ifs, range.start_offset, std::ios::beg);
    if (file_ops_->ifs_fail(*ifs)) {
        int saved_errno = errno;
        return backend_shared_impl::make_stream_error_status("failed to seek to start_offset", abs_path, range.start_offset, saved_errno);
    }

    std::vector<char> buffer(chunk_size_);
    std::streamsize offset = range.start_offset;
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
            return {::grpc::StatusCode::UNKNOWN, "stream write failed"};
        }
        offset += bytes_read;
        remaining -= bytes_read;
        is_first = false;
    }
    if (offset < range.start_offset + send_size) {
        return {::grpc::StatusCode::DATA_LOSS, "file truncated during read: " + abs_path.string()};
    }
    return ::grpc::Status::OK;
}

::grpc::Status backend_shared_impl::make_stream_error_status(const std::string& context, const boost::filesystem::path& path, std::optional<std::streamoff> offset, int err) {
    std::string err_msg = context + ": " + path.string();
    if (offset) {
        err_msg += ", offset=" + std::to_string(*offset);
    }
    err_msg += ", errno=" + std::to_string(err) + ", " + std::strerror(err);
    ::grpc::StatusCode code = ::grpc::StatusCode::INTERNAL;
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
    return {code, err_msg};
}

void backend_shared_impl::reset_file_operations_to_default() noexcept {
    file_ops_ = default_file_ops_.get();
}

::grpc::Status backend_shared_impl::begin_backup(datastore& datastore_, limestone::grpc::proto::BeginBackupRequest const* request,
                                                 limestone::grpc::proto::BeginBackupResponse* response, backup_path_list_provider_type const& backup_path_list_provider) noexcept {
    if (!backup_path_list_provider) {
        return {::grpc::StatusCode::INTERNAL, 
                "Unexpected error: backup_path_list_provider is not set. This may indicate an issue in the server implementation."};
    }
    try {
		// Call the exception injection hook if it is set (for testing)
        if (exception_hook_) {
            exception_hook_();
        }
        if (request->version() != begin_backup_message_version) {
            return {::grpc::StatusCode::INVALID_ARGUMENT, std::string("unsupported begin_backup request version: ") + std::to_string(request->version())};
        }
        uint32_t begin_epoch = request->begin_epoch();
        uint32_t end_epoch = request->end_epoch();

        // Create a session for this backup. The second argument is a callback
        // that will be invoked when the session is removed (expired or deleted).
        // In this callback, decrement_backup_counter() is called to update the backup counter.
        auto session = create_and_register_session(
            begin_epoch,
            end_epoch,
            session_timeout_seconds,
            [&datastore_]() {
                datastore_.get_impl()->decrement_backup_counter();
            }
        );

        // Handle error if session creation fails
        if (!session) {
            // Collision of session_id (UUID) is practically impossible, so this branch is unreachable in normal operation.
            return {::grpc::StatusCode::INTERNAL, "failed to create session"};
        }

        // Use a scope guard to ensure the session is removed in case of an error.
        // This guarantees that the session will not remain active if an exception occurs.
        struct session_remover {
        private:
            backend_shared_impl* backend;
            std::string session_id;

        public:
            session_remover(backend_shared_impl* backend, std::string session_id)
                : backend(backend), session_id(std::move(session_id)) {}

            void operator()(limestone::grpc::backend::session* s) const {
                if (s) {
                    backend->get_session_store().remove_session(session_id);
                }
            }
        };
        auto session_guard = std::unique_ptr<limestone::grpc::backend::session, session_remover>(
            &(*session), // Extract the raw pointer from std::optional
            session_remover{this, session->session_id()}
        );

        // Validate the backup parameters
        compaction_catalog& catalog = datastore_.get_impl()->get_compaction_catalog();
        bool is_full_backup = (begin_epoch == 0 && end_epoch == 0);
        if (!is_full_backup) {
            // differential backup
            if (begin_epoch >= end_epoch) {
                return {::grpc::StatusCode::INVALID_ARGUMENT,
                        "begin_epoch must be less than end_epoch: begin_epoch=" + std::to_string(begin_epoch) + ", end_epoch=" + std::to_string(end_epoch)};
            }
            auto snapshot_epoch_id = catalog.get_max_epoch_id();
            if (begin_epoch <= snapshot_epoch_id) {
                return {::grpc::StatusCode::INVALID_ARGUMENT, "begin_epoch must be strictly greater than the epoch id of the last snapshot: begin_epoch=" +
                                                                  std::to_string(begin_epoch) + ", snapshot_epoch_id=" + std::to_string(snapshot_epoch_id)};
            }
            auto current_epoch_id = datastore_.last_epoch();
            if (end_epoch > current_epoch_id) {
                return {::grpc::StatusCode::INVALID_ARGUMENT, "end_epoch must be less than or equal to the current epoch id: end_epoch=" +
                                                                  std::to_string(end_epoch) + ", current_epoch_id=" + std::to_string(current_epoch_id)};
            }
            auto boot_durable_epoch_id = datastore_.get_impl()->get_boot_durable_epoch_id();
            if (end_epoch < boot_durable_epoch_id) {
                return {::grpc::StatusCode::INVALID_ARGUMENT, "end_epoch must be strictly greater than the durable epoch id at boot time: end_epoch=" +
                                                                  std::to_string(end_epoch) + ", boot_durable_epoch_id=" + std::to_string(boot_durable_epoch_id)};
            }
        }


        // Use custom path provider if provided, otherwise use default implementation
        auto paths = backup_path_list_provider();

        auto backup_objects = backend_shared_impl::generate_backup_objects(paths, is_full_backup);
        for (const auto& obj : backup_objects) {
            session_store_.add_backup_object_to_session(session->session_id(), obj);
            *response->add_objects() = obj.to_proto();
        }

        // Release the scope guard before returning success.
        // This ensures the session remains valid after the function exits successfully.
        [[maybe_unused]] auto unused = session_guard.release();

        response->set_session_id(session->session_id());
        response->set_expire_at(session->expire_at());
        response->set_start_epoch(begin_epoch);
        response->set_finish_epoch(end_epoch);
        return ::grpc::Status::OK;
    } catch (const std::exception& e) {
        VLOG_LP(log_info) << "begin_backup failed: " << e.what();
        std::string msg = e.what();
        return {::grpc::StatusCode::INTERNAL, msg};
    }
}

}  // namespace limestone::grpc::backend
