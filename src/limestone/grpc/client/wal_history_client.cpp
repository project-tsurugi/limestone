#include <grpc/client/wal_history_client.h>
#include <glog/logging.h>
#include <chrono>

namespace limestone::grpc::client {

wal_history_client::wal_history_client(std::string const& server_address)
    : stub_(WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address, ::grpc::InsecureChannelCredentials()))) {
    LOG(INFO) << "wal_history_client created for server: " << server_address;
}

wal_history_client::wal_history_client(std::shared_ptr<::grpc::Channel> const& channel)
    : stub_(WalHistoryService::NewStub(channel)) {
    LOG(INFO) << "wal_history_client created with custom channel";
}

::grpc::Status wal_history_client::get_wal_history(WalHistoryRequest const& request, WalHistoryResponse& response) {
    ::grpc::ClientContext context;
    LOG(INFO) << "Sending GetWalHistory request.";
    ::grpc::Status status = stub_->GetWalHistory(&context, request, &response);
    if (status.ok()) {
        LOG(INFO) << "GetWalHistory response received.";
    } else {
        LOG(ERROR) << "GetWalHistory RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

::grpc::Status wal_history_client::get_wal_history(WalHistoryRequest const& request,
                                                   WalHistoryResponse& response,
                                                   int timeout_ms) {
    ::grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    LOG(INFO) << "Sending GetWalHistory request with timeout " << timeout_ms << "ms.";
    ::grpc::Status status = stub_->GetWalHistory(&context, request, &response);
    if (status.ok()) {
        LOG(INFO) << "GetWalHistory response received.";
    } else {
        LOG(ERROR) << "GetWalHistory RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

} // namespace limestone::grpc::client
