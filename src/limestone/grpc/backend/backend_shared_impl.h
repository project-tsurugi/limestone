#pragma once

#include <google/protobuf/repeated_field.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "backup.grpc.pb.h"
#include "grpc/service/grpc_constants.h"
#include "limestone/api/blob_id_type.h"
#include "session.h"
#include "session_store.h"
#include "wal_history.grpc.pb.h"
#include "wal_sync/wal_history.h"
#include "limestone/api/datastore.h"

namespace limestone::grpc::backend {

using limestone::grpc::proto::BranchEpoch;
using limestone::grpc::proto::BackupObject;
using limestone::api::blob_id_type;
using limestone::api::datastore;

/**
 * @brief Type for backup path list provider function
 * 
 * A function that takes datastore and returns a vector of filesystem paths.
 * This allows customization of how paths are extracted from datastore.
 */
using backup_path_list_provider_type = std::function<std::vector<boost::filesystem::path>()>;

class i_writer {
public:
    i_writer() = default; 
    virtual ~i_writer() = default;
    i_writer(const i_writer&) = delete;
    i_writer& operator=(const i_writer&) = delete;
    i_writer(i_writer&&) = delete;
    i_writer& operator=(i_writer&&) = delete;
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


/**
 * @brief Represents a byte range within a file to be copied.
 *
 * This structure specifies the range of bytes in a file that should be copied.
 * - start_offset: The starting byte offset (inclusive).
 * - end_offset: The ending byte offset (exclusive). If end_offset is 0, no bytes are copied.
 *   If end_offset is std::nullopt, the range extends to the end of the file.
 */
struct byte_range {
    std::streamoff start_offset = 0; ///< Start position (inclusive)
    std::optional<std::streamoff> end_offset = std::nullopt; ///< End position (exclusive). If nullopt, means EOF.
};

class backend_shared_impl {
public:
    explicit backend_shared_impl(boost::filesystem::path log_dir, std::size_t chunk_size = limestone::grpc::service::backup_object_chunk_size);
    virtual ~backend_shared_impl() = default;
    backend_shared_impl(const backend_shared_impl&) = delete;
    backend_shared_impl& operator=(const backend_shared_impl&) = delete;
    backend_shared_impl(backend_shared_impl&&) = delete;
    backend_shared_impl& operator=(backend_shared_impl&&) = delete;

    // Shared logic for listing WAL history (returns proto repeated field)
    google::protobuf::RepeatedPtrField<BranchEpoch> list_wal_history();

    // Shared logic for creating backup objects from file paths
    static std::vector<backup_object> generate_backup_objects(const std::vector<boost::filesystem::path>& paths, bool is_full_backup);

    // Create and register a session via session_store, return the created session
    std::optional<session> create_and_register_session(epoch_id_type begin_epoch, epoch_id_type end_epoch, int64_t timeout_seconds, session::on_remove_callback_type on_remove = nullptr);

    // Shared logic for keep_alive
    ::grpc::Status keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept;

    // Shared logic for end_backup
    ::grpc::Status end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept;

    // Get backup objects
    ::grpc::Status get_object(const limestone::grpc::proto::GetObjectRequest* request, i_writer* writer) noexcept;

    // Shared logic for begin backup
    ::grpc::Status begin_backup(datastore& datastore_, const limestone::grpc::proto::BeginBackupRequest* request,
                                limestone::grpc::proto::BeginBackupResponse* response, backup_path_list_provider_type const&backup_path_list_provider) noexcept;

    /**
     * @brief Send backup object data as a chunked gRPC stream.
     *
     * This function streams the contents of the specified backup object file in chunks.
     * The byte range to send is specified by the byte_range struct.
     * If end_offset is not specified, the file is sent until the end.
     *
     * The range is [range.start_offset, range.end_offset):
     *   - start_offset: inclusive (the first byte to send)
     *   - end_offset: exclusive (the first byte NOT to send)
     *
     * @param object     The backup object to send.
     * @param writer     The gRPC ServerWriter to stream the data.
     * @param range      The byte range to send. If end_offset is not specified, sends to the end of the file.
     * @return ::grpc::Status  gRPC status indicating success or error reason.
     */
    ::grpc::Status send_backup_object_data(
        const backup_object& object,
        i_writer* writer,
        const byte_range& range
    );


    /**
     * @brief Prepares a copy of a log object for backup within a specified epoch range.
     *
     * This function creates a byte range representing the portion of the log object
     * that falls between the given begin and end epochs. It also inserts into the
     * provided set the blob_id of any blob file that may need to be copied.
     * If an error occurs, returns std::nullopt and sets error_status.
     *
     * @param object The backup_object to be copied.
     * @param begin_epoch The starting epoch (inclusive) for the copy operation.
     * @param end_epoch The ending epoch (exclusive) for the copy operation.
     * @param required_blobs Reference to a set to which blob_ids that may need to be copied will be inserted.
     * @param error_status Set to error detail if an error occurs.
     * @return std::optional<byte_range> The range of bytes representing the copied log object data, or std::nullopt on error.
     */
    std::optional<byte_range> prepare_log_object_copy(
        const backup_object& object,
        epoch_id_type begin_epoch,
        epoch_id_type end_epoch,
        std::set<blob_id_type>& required_blobs,
        ::grpc::Status& error_status
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

    // For testing: set exception injection hook
    void set_exception_hook(std::function<void()> hook) { exception_hook_ = std::move(hook); }
private:
    std::function<void()> exception_hook_;
    boost::filesystem::path log_dir_;
    session_store session_store_;
    std::size_t chunk_size_;
    std::unique_ptr<limestone::internal::file_operations> default_file_ops_;
    limestone::internal::file_operations* file_ops_ = nullptr;
};

} // namespace limestone::grpc::backend
