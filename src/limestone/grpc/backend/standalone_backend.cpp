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
namespace limestone::grpc::backend {



standalone_backend::standalone_backend(const boost::filesystem::path& log_dir)
	: log_dir_(log_dir), backend_shared_impl_(log_dir)
{
}

::grpc::Status standalone_backend::get_wal_history_response(WalHistoryResponse* response) noexcept {
    try {
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

boost::filesystem::path standalone_backend::get_log_dir() const noexcept {
	return log_dir_;
}

} // namespace limestone::grpc::backend
