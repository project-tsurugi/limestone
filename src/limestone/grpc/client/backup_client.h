#pragma once

#include <grpcpp/grpcpp.h>

#include <functional>

#include "backup.grpc.pb.h"

namespace limestone::grpc::client {

using limestone::grpc::proto::BackupService;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::proto::KeepAliveRequest;
using limestone::grpc::proto::KeepAliveResponse;
using limestone::grpc::proto::EndBackupRequest;
using limestone::grpc::proto::EndBackupResponse;
using limestone::grpc::proto::GetObjectRequest;
using limestone::grpc::proto::GetObjectResponse;    

class backup_client {
public:
    /**
     * @brief Construct backup_client with server address.
     * @param server_address Address of the gRPC server.
     */
    explicit backup_client(std::string const& server_address);

    /**
     * @brief Construct backup_client with a custom gRPC channel.
     * @param channel Shared pointer to a gRPC channel.
     */
    explicit backup_client(std::shared_ptr<::grpc::Channel> const& channel);

    /**
     * @brief Send BeginBackup request.
     * @param request BeginBackupRequest message.
     * @param response BeginBackupResponse message.
     * @param timeout_ms Timeout in milliseconds.
     * @return gRPC status of the operation.
     */
    ::grpc::Status begin_backup(BeginBackupRequest const& request,
                                BeginBackupResponse& response,
                                int timeout_ms);

    /**
     * @brief Send KeepAlive request.
     * @param request KeepAliveRequest message.
     * @param response KeepAliveResponse message.
     * @param timeout_ms Timeout in milliseconds.
     * @return gRPC status of the operation.
     */
    ::grpc::Status keep_alive(KeepAliveRequest const& request,
                              KeepAliveResponse& response,
                              int timeout_ms);

    /**
     * @brief Send EndBackup request.
     * @param request EndBackupRequest message.
     * @param response EndBackupResponse message.
     * @param timeout_ms Timeout in milliseconds.
     * @return gRPC status of the operation.
     */
    ::grpc::Status end_backup(EndBackupRequest const& request,
                              EndBackupResponse& response,
                              int timeout_ms);

    /**
     * @brief Send GetObject request and handle streaming responses.
     * @param request GetObjectRequest message.
     * @param handler Callback function to handle each GetObjectResponse.
     * @param timeout_ms Timeout in milliseconds.
     * @return gRPC status of the operation.
     */
    ::grpc::Status get_object(GetObjectRequest const& request,
                              std::function<void(GetObjectResponse const&)> const& handler,
                              int timeout_ms);

private:
    std::unique_ptr<BackupService::Stub> stub_;
};

} // namespace limestone::grpc::client