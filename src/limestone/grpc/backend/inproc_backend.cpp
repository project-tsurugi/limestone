/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "inproc_backend.h"

#include "grpc/service/grpc_constants.h"
#include "grpc/service/message_versions.h"
#include "limestone/logging.h"
#include "logging_helper.h"
#include "session.h"
namespace limestone::grpc::backend {

using limestone::grpc::service::list_wal_history_message_version;

inproc_backend::inproc_backend([[maybe_unused]] limestone::api::datastore& ds, const boost::filesystem::path& log_dir)
	: datastore_(ds), log_dir_(log_dir), backend_shared_impl_(log_dir)
{
}

::grpc::Status inproc_backend::get_wal_history_response(const WalHistoryRequest* request, WalHistoryResponse* response) noexcept {
	try {
		// Call the exception injection hook if it is set (for testing)
        if (exception_hook_) {
            exception_hook_();
        }
        if (request->version() != list_wal_history_message_version) {
            return {::grpc::StatusCode::INVALID_ARGUMENT, std::string("unsupported wal history request version: ") + std::to_string(request->version())};
        }

		response->set_last_epoch(datastore_.last_epoch());
		*response->mutable_records() = backend_shared_impl_.list_wal_history();
		return {::grpc::Status::OK};
	} catch (const std::exception& e) {
		VLOG_LP(log_info) << "GetWalHistory failed: " << e.what();
		std::string msg = e.what();
		return {::grpc::StatusCode::INTERNAL, msg};
	}
}

::grpc::Status inproc_backend::begin_backup(const BeginBackupRequest* request, BeginBackupResponse* response) noexcept {
    return backend_shared_impl_.begin_backup(datastore_, request, response);
}

boost::filesystem::path inproc_backend::get_log_dir() const noexcept {
	return log_dir_;
}


::grpc::Status inproc_backend::keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept {
    return backend_shared_impl_.keep_alive(request, response);
}

::grpc::Status inproc_backend::end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept {
    return backend_shared_impl_.end_backup(request, response);
}

::grpc::Status inproc_backend::get_object(const limestone::grpc::proto::GetObjectRequest* request, ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept {
    grpc_writer_adapter adapter(writer);
    return backend_shared_impl_.get_object(request, &adapter);
}

backend_shared_impl& inproc_backend::get_backend_shared_impl() noexcept {
    return backend_shared_impl_;
}

} // namespace limestone::grpc::backend

