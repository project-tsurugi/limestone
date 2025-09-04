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
#include "standalone_backend.h"
#include "dblog_scan.h"
#include "limestone_exception_helper.h"
#include "limestone/logging.h"
#include "logging_helper.h"
#include "grpc/service/message_versions.h"
namespace limestone::grpc::backend {


using limestone::grpc::service::list_wal_history_message_version;



standalone_backend::standalone_backend(const boost::filesystem::path& log_dir)
    : log_dir_(log_dir)
    , backend_shared_impl_(log_dir)
    , datastore_([&]{
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(log_dir);
    limestone::api::configuration conf(data_locations, log_dir);
        return limestone::api::datastore(conf);
    }())
{
}

::grpc::Status standalone_backend::get_wal_history_response(const WalHistoryRequest* request, WalHistoryResponse* response) noexcept {
    try {
        if (request->version() != list_wal_history_message_version) {
            return {::grpc::StatusCode::INVALID_ARGUMENT, std::string("unsupported wal history request version: ") + std::to_string(request->version())};
        }

        limestone::internal::dblog_scan scan(log_dir_);
        auto last_epoch = scan.last_durable_epoch_in_dir();
        response->set_last_epoch(last_epoch);
        auto records = backend_shared_impl_.list_wal_history();
        for (const auto& rec : records) {
            if (rec.epoch() > last_epoch) {
                LOG_AND_THROW_EXCEPTION("wal history contains a record whose epoch is greater than last_epoch: epoch=" + std::to_string(rec.epoch()) +
                                        ", last_epoch=" + std::to_string(last_epoch));
            }
        }
        *response->mutable_records() = std::move(records);
        return {::grpc::Status::OK};
    } catch (const std::exception& e) {
        VLOG_LP(log_info) << "GetWalHistory failed: " << e.what();
        std::string msg = e.what();
        return {::grpc::StatusCode::INTERNAL, msg};
    }
}


::grpc::Status standalone_backend::begin_backup(const BeginBackupRequest* request, BeginBackupResponse* response) noexcept {
    return backend_shared_impl_.begin_backup(datastore_, request, response);
}



boost::filesystem::path standalone_backend::get_log_dir() const noexcept {
	return log_dir_;
}


::grpc::Status standalone_backend::keep_alive(const limestone::grpc::proto::KeepAliveRequest* request, limestone::grpc::proto::KeepAliveResponse* response) noexcept {
    return backend_shared_impl_.keep_alive(request, response);
}

::grpc::Status standalone_backend::end_backup(const limestone::grpc::proto::EndBackupRequest* request, limestone::grpc::proto::EndBackupResponse* response) noexcept {
    return backend_shared_impl_.end_backup(request, response);
}

::grpc::Status standalone_backend::get_object(const limestone::grpc::proto::GetObjectRequest* request, ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer) noexcept {
    grpc_writer_adapter adapter(writer);
    return backend_shared_impl_.get_object(request, &adapter);
}

} // namespace limestone::grpc::backend
