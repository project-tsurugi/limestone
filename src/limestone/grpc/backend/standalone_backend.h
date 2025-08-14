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
#pragma once

#include <vector>
#include "grpc_service_backend.h"
#include "backend_shared_impl.h"
namespace limestone::grpc::backend {

class standalone_backend : public grpc_service_backend {
public:
    explicit standalone_backend(const boost::filesystem::path& log_dir);
    ~standalone_backend() override = default;

    standalone_backend(const standalone_backend&) = delete;
    standalone_backend& operator=(const standalone_backend&) = delete;
    standalone_backend(standalone_backend&&) = delete;
    standalone_backend& operator=(standalone_backend&&) = delete;

    [[nodiscard]] std::vector<wal_history::record> list_wal_history() override;
    [[nodiscard]] boost::filesystem::path get_log_dir() const noexcept override;
private:
    boost::filesystem::path log_dir_;
    backend_shared_impl backend_shared_impl_;
};

} // namespace limestone::grpc::backend
