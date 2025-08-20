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
#include "limestone/logging.h"
#include "logging_helper.h"
#include "grpc/service/message_versions.h"
namespace limestone::grpc::backend {

using limestone::grpc::service::list_wal_history_message_version;

::grpc::Status inproc_backend::begin_backup(BeginBackupRequest* /*request*/, BeginBackupResponse* /*response*/) noexcept {
	return {::grpc::StatusCode::UNIMPLEMENTED, "begin_backup not implemented"};
}



inproc_backend::inproc_backend([[maybe_unused]] limestone::api::datastore& ds, const boost::filesystem::path& log_dir)
	: datastore_(ds), log_dir_(log_dir), backend_shared_impl_(log_dir)
{
}

::grpc::Status inproc_backend::get_wal_history_response(const WalHistoryRequest* request, WalHistoryResponse* response) noexcept {
	try {
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

boost::filesystem::path inproc_backend::get_log_dir() const noexcept {
	return log_dir_;
}

} // namespace limestone::grpc::backend
