
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
#include "grpc_service_backend.h"
#include "inproc_backend.h"
#include "standalone_backend.h"


namespace limestone::grpc::backend {

using limestone::api::datastore;    

std::unique_ptr<grpc_service_backend> grpc_service_backend::create_inproc(datastore& store, const boost::filesystem::path& log_dir) {
	return std::make_unique<inproc_backend>(store, log_dir);
}

std::unique_ptr<grpc_service_backend> grpc_service_backend::create_standalone(const boost::filesystem::path& log_dir) {
	return std::make_unique<standalone_backend>(log_dir);
}

} // namespace limestone::grpc::backend
