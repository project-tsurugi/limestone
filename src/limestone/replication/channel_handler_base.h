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
#include "replication_message.h"
#include "socket_io.h"
#include "message_ack.h"
#include "message_error.h"
#include "validation_result.h"
#include "replica_server.h"

namespace limestone::replication {

class channel_handler_base {
public:
    explicit channel_handler_base(replica_server& server) noexcept;

    // Validate first request, send initial ACK or Error, then start processing loop
    void run(socket_io& io, std::unique_ptr<replication_message> first_request);

protected:
    // Validate initial request; return ok() or error()
    virtual validation_result validate_initial(std::unique_ptr<replication_message> request) = 0;

    // Send initial ACK; must be implemented by subclass
    virtual void send_initial_ack(socket_io& io) const = 0;

    // Send generic ACK anywhere (e.g., within processing loop)
    void send_ack(socket_io& io) const;

    // Send error response
    void send_error(socket_io& io, const validation_result& result) const;

    // Handle subsequent messages in the processing loop
    virtual void dispatch(replication_message& message, socket_io& io) = 0;

    // Provide thread name for the current channel handler
    virtual const char* thread_name() const = 0;

    // Main receive-dispatch loop
    void process_loop(socket_io& io);

    replica_server& server_;
};

}  // namespace limestone::replication
