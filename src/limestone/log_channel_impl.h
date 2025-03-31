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

#include <memory>
#include <vector>
#include <string_view>
#include <boost/filesystem.hpp>

#include "limestone/api/blob_id_type.h"
#include "limestone/api/storage_id_type.h"
#include "limestone/api/write_version_type.h"
#include "limestone/status.h"
#include "replication/replica_connector.h"
#include "replication/message_log_entries.h"

namespace limestone::api {

class datastore;

class log_channel_impl {
public:
    log_channel_impl(datastore& envelope);
    ~log_channel_impl() = default;

    /**
     * @brief Sets the replica connector instance. Ownership is transferred.
     * @param connector A unique_ptr to a replica_connector instance.
     * @note This method is for internal use only and is not part of the public API.
     *       Do not use this method in production code.
     */
    void set_replica_connector(std::unique_ptr<replication::replica_connector> connector);

    /**
     * @brief Disables the current replica connector by resetting the unique_ptr.
     * @note This method is for internal use only and is not part of the public API.
     *       Do not use this method in production code.
     */
    void disable_replica_connector();

    /**
     * @brief Test-only getter for the replica_connector instance.
     * @return A raw pointer to the replica_connector instance.
     */
    replication::replica_connector* get_replica_connector();


    void send_replica_message(uint64_t epoch_id, const std::function<void(replication::message_log_entries&)>& modifier);
  


private:
    std::unique_ptr<replication::replica_connector> replica_connector_;
    std::mutex mtx_replica_connector_;
    datastore& envelope_;
};

}  // namespace limestone::api
