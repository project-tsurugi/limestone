#include <grpc/client/backup_client.h>
#include <glog/logging.h>
#include <chrono>

namespace limestone::grpc::client {

backup_client::backup_client(std::string const& server_address)
    : stub_(BackupService::NewStub(
        ::grpc::CreateChannel(server_address, ::grpc::InsecureChannelCredentials()))) {
    LOG(INFO) << "backup_client created for server: " << server_address;
}

backup_client::backup_client(std::shared_ptr<::grpc::Channel> const& channel)
    : stub_(BackupService::NewStub(channel)) {
    LOG(INFO) << "backup_client created with custom channel";
}

::grpc::Status backup_client::begin_backup(BeginBackupRequest const& request,
                                           BeginBackupResponse& response,
                                           int timeout_ms) {
    ::grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    LOG(INFO) << "Sending BeginBackup request with timeout " << timeout_ms << "ms.";
    ::grpc::Status status = stub_->BeginBackup(&context, request, &response);
    if (status.ok()) {
        LOG(INFO) << "BeginBackup response received.";
    } else {
        LOG(ERROR) << "BeginBackup RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

::grpc::Status backup_client::keep_alive(KeepAliveRequest const& request,
                                         KeepAliveResponse& response,
                                         int timeout_ms) {
    ::grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    LOG(INFO) << "Sending KeepAlive request with timeout " << timeout_ms << "ms.";
    ::grpc::Status status = stub_->KeepAlive(&context, request, &response);
    if (status.ok()) {
        LOG(INFO) << "KeepAlive response received.";
    } else {
        LOG(ERROR) << "KeepAlive RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

::grpc::Status backup_client::end_backup(EndBackupRequest const& request,
                                         EndBackupResponse& response,
                                         int timeout_ms) {
    ::grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    LOG(INFO) << "Sending EndBackup request with timeout " << timeout_ms << "ms.";
    ::grpc::Status status = stub_->EndBackup(&context, request, &response);
    if (status.ok()) {
        LOG(INFO) << "EndBackup response received.";
    } else {
        LOG(ERROR) << "EndBackup RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

::grpc::Status backup_client::get_object(GetObjectRequest const& request,
                                         std::function<void(GetObjectResponse const&)> const& handler,
                                         int timeout_ms) {
    ::grpc::ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    context.set_deadline(deadline);
    LOG(INFO) << "Sending GetObject request with timeout " << timeout_ms << "ms.";
    auto reader = stub_->GetObject(&context, request);
    GetObjectResponse response;
    while (reader->Read(&response)) {
        // FIXME: Implement file writing logic here.
        handler(response);
    }
    ::grpc::Status status = reader->Finish();
    if (status.ok()) {
        LOG(INFO) << "GetObject stream completed successfully.";
    } else {
        LOG(ERROR) << "GetObject RPC failed: " << status.error_code()
                   << ": " << status.error_message();
    }
    return status;
}

} // namespace limestone::grpc::client