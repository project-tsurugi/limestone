/*
 * Copyright 2026-2026 Project Tsurugi.
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

#include "replication_message_io.h"
#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>
#include <cstdio>

namespace limestone::replication {

using limestone::api::datastore;
using limestone::api::blob_id_type;

/**
 * @brief replication_message_io extension that can send and receive BLOB payloads on the TCP path.
 *
 * TCP replication still deserializes LOG_ENTRY messages via
 * replication_message::receive() / message_log_entries::receive_body(), so BLOB
 * payload handling remains here.
 */
class tcp_replication_message_io : public replication_message_io {
public:
    /**
     * @brief Maximum BLOB payload chunk size used for TCP send/receive buffers, in bytes.
     */
    static constexpr std::size_t blob_buffer_size = 64UL * 1024UL;
    
    // Disallow copy and move operations
    tcp_replication_message_io(const tcp_replication_message_io&) = delete;
    tcp_replication_message_io& operator=(const tcp_replication_message_io&) = delete;
    tcp_replication_message_io(tcp_replication_message_io&&) = delete;
    tcp_replication_message_io& operator=(tcp_replication_message_io&&) = delete;

    // Default constructor
    ~tcp_replication_message_io() override = default;

    // Real‑socket constructor
    tcp_replication_message_io(int fd, datastore& ds);

    // String‑mode constructor (for tests)
    tcp_replication_message_io(const std::string& initial, datastore& ds);

    // Send/receive blob methods
    void send_blob(blob_id_type blob_id) override;
    blob_id_type receive_blob() override;
private:
    void safe_close(FILE *fp);
    datastore& datastore_;
};

} // namespace limestone::replication
