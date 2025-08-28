#pragma once

#include <google/protobuf/repeated_field.h>

#include <optional>
#include <vector>
#include <memory>

#include "backup.grpc.pb.h"
#include "grpc/service/grpc_constants.h"
#include "session.h"
#include "session_store.h"
#include "wal_history.grpc.pb.h"
#include "wal_sync/wal_history.h"
namespace limestone::grpc::backend {

using limestone::grpc::proto::BranchEpoch;
using limestone::grpc::proto::BackupObject;

class i_writer {
public:
    virtual ~i_writer() = default;
    virtual bool Write(const limestone::grpc::proto::GetObjectResponse& resp) = 0;
};

class grpc_writer_adapter : public i_writer {
public:
    explicit grpc_writer_adapter(::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* w) : writer_(w) {}
    bool Write(const limestone::grpc::proto::GetObjectResponse& resp) override {
        return writer_->Write(resp);
    }
private:
    ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer_;
};


class backend_shared_impl {
public:
    explicit backend_shared_impl(const boost::filesystem::path& log_dir, std::size_t chunk_size = limestone::grpc::service::backup_object_chunk_size);
    virtual ~backend_shared_impl() = default;
    backend_shared_impl(const backend_shared_impl&) = delete;
    backend_shared_impl& operator=(const backend_shared_impl&) = delete;
    backend_shared_impl(backend_shared_impl&&) = delete;
    backend_shared_impl& operator=(backend_shared_impl&&) = delete;

    // Shared logic for listing WAL history (returns proto repeated field)
    google::protobuf::RepeatedPtrField<BranchEpoch> list_wal_history();

    // Shared logic for creating backup objects from file paths
    static std::optional<backup_object> make_backup_object_from_path(const boost::filesystem::path& path);

    // Create and register a session via session_store, return the created session
    std::optional<session> create_and_register_session(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, session::on_remove_callback_type on_remove = nullptr);

    // Shared logic for keep_alive
    ::grpc::Status keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept;

    // Shared logic for end_backup
    ::grpc::Status end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept;

    // Get backup objects
    ::grpc::Status get_object(const limestone::grpc::proto::GetObjectRequest* request, ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept;

    /**
     * @brief Send backup object data as a chunked gRPC stream.
     *
     * This function streams the contents of the specified backup object file in chunks.
     * The byte range to send can be specified by start_offset and end_offset.
     * If end_offset is not specified, the file is sent until the end.
     *
     * The range is [start_offset, end_offset):
     *   - start_offset: inclusive (the first byte to send)
     *   - end_offset: exclusive (the first byte NOT to send)
     *
     * @param object        The backup object to send.
     * @param writer        The gRPC ServerWriter to stream the data.
     * @param start_offset  The starting byte offset (inclusive) from which to begin sending data. Default is 0.
     * @param end_offset    The ending byte offset (exclusive) at which to stop sending data. If not specified, sends to the end of the file.
     * @return ::grpc::Status  gRPC status indicating success or error reason.
     */
    ::grpc::Status send_backup_object_data(
        const backup_object& object,
        i_writer* writer,
        std::streamoff start_offset = 0,
        std::optional<std::streamoff> end_offset = std::nullopt
    );

    // Getter for session_store
    session_store& get_session_store() noexcept;

    /**
     * @brief Set file operations implementation for testing.
     *
     * This method allows injecting a mock file_operations implementation for unit testing.
     * The caller is responsible for the lifetime of the injected object.
     *
     * @param file_ops  Pointer to the file_operations implementation to use. Must not be nullptr.
     */
    void set_file_operations(limestone::internal::file_operations* file_ops) noexcept;

    /**
     * @brief Reset file_ops_ to default_file_ops_.
     *
     * This method sets file_ops_ to point to the default file_operations instance.
     * Intended for use in test or after custom file_ops_ injection.
     */
    void reset_file_operations_to_default() noexcept;

    /**
     * @brief Create a gRPC error status for file stream errors with detailed context.
     *
     * This utility is public only for unit testing. It is not intended for use outside this class except for tests.
     *
     * @param context  Description of the operation that failed (e.g., "failed to open file").
     * @param path     The file path involved in the error.
     * @param offset   Optional file offset related to the error (e.g., for seek failures).
     * @param err      The errno value captured at the point of failure.
     * @return ::grpc::Status  gRPC status with appropriate error code and message.
     */
    static ::grpc::Status make_stream_error_status(const std::string& context, const boost::filesystem::path& path, std::optional<std::streamoff> offset, int err);

private:
    boost::filesystem::path log_dir_;
    session_store session_store_;
    std::size_t chunk_size_;
    std::unique_ptr<limestone::internal::file_operations> default_file_ops_;
    limestone::internal::file_operations* file_ops_ = nullptr;
};

} // namespace limestone::grpc::backend
