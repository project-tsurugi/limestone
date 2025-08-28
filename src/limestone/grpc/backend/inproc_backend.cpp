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

#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "grpc/service/grpc_constants.h"
#include "grpc/service/message_versions.h"
#include "limestone/api/backup_detail.h"
#include "limestone/logging.h"
#include "logging_helper.h"
#include "session.h"
namespace limestone::grpc::backend {

using limestone::api::backup_detail_and_rotation_result;
using limestone::api::backup_type;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::list_wal_history_message_version;
using limestone::grpc::service::session_timeout_seconds;
using limestone::internal::compaction_catalog;

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
        auto session = backend_shared_impl_.create_and_register_session(
            begin_epoch,
            end_epoch,
            session_timeout_seconds,
            [this]() {
                datastore_.get_impl()->decrement_backup_counter();
            }
        );
        if (!session) {
            return {::grpc::StatusCode::INTERNAL, "failed to create session"};
        }

        response->set_session_id(session->session_id());
        response->set_expire_at(session->expire_at());
        bool is_full_backup = (begin_epoch == 0 && end_epoch == 0);
        compaction_catalog& catalog = datastore_.get_impl()->get_compaction_catalog();
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
        response->set_start_epoch(begin_epoch);
        response->set_finish_epoch(end_epoch);

        backup_detail_and_rotation_result result = datastore_.get_impl()->begin_backup_with_rotation_result(backup_type::transaction);
        if (result.detail) {
            for (const auto& entry : result.detail->entries()) {
                auto obj = backend_shared_impl::make_backup_object_from_path(entry.source_path());
                if (obj) {
                    backend_shared_impl_.get_session_store().add_backup_object_to_session(session->session_id(), *obj);
                    *response->add_objects() = obj->to_proto();
                }
            }
        }
        return ::grpc::Status::OK;
    } catch (const std::exception& e) {
        VLOG_LP(log_info) << "begin_backup failed: " << e.what();
        std::string msg = e.what();
        return {::grpc::StatusCode::INTERNAL, msg};
    }
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
    return backend_shared_impl_.get_object(request, writer);
}

backend_shared_impl& inproc_backend::get_backend_shared_impl() noexcept {
    return backend_shared_impl_;
}

} // namespace limestone::grpc::backend

