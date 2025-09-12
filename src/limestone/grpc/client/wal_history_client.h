#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "wal_history.grpc.pb.h"

namespace limestone::grpc::client {

using limestone::grpc::proto::WalHistoryService;
using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;

/**
 * @brief WAL history service client for retrieving WAL records.
 *
 * This client provides a convenient interface to communicate with
 * the WalHistoryService over gRPC.
 */
class wal_history_client {
public:
    /**
     * @brief Construct wal_history_client with server address
     *
     * @param server_address Server address in format "host:port"
     */
    explicit wal_history_client(std::string const& server_address);

    /**
     * @brief Constructor with custom gRPC channel
     * @param channel Shared pointer to gRPC channel
     */
    explicit wal_history_client(std::shared_ptr<::grpc::Channel> const& channel);

    /**
     * @brief Get WAL history with timeout
     * @param request WAL history request
     * @param response Reference to store the response
     * @param timeout_ms Timeout in milliseconds
     * @return gRPC status of the operation
     */
    ::grpc::Status get_wal_history(WalHistoryRequest const& request,
                                   WalHistoryResponse& response,
                                   int timeout_ms);

private:
    std::unique_ptr<WalHistoryService::Stub> stub_;
};

} // namespace limestone::grpc::client
