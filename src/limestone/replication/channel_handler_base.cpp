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

#include "replication/channel_handler_base.h"

#include <pthread.h>

namespace limestone::replication {

channel_handler_base::channel_handler_base(replica_server& server) noexcept : server_(server) {}

void channel_handler_base::run(socket_io& io, std::unique_ptr<replication_message> first_request) {
    auto assignment_result = authorize();
    if (!assignment_result.ok()) {
        send_error(io, assignment_result);
        return;
    }

    auto validation_result = validate_initial(std::move(first_request));
    if (!validation_result.ok()) {
        send_error(io, validation_result);
        return;
    }

    send_initial_ack(io);
    process_loop(io);
}

void channel_handler_base::send_ack(socket_io& io) const {
    message_ack ack;
    replication_message::send(io, ack);
    io.flush();
}

void channel_handler_base::send_error(socket_io& io, const validation_result& result) const {
    message_error err;
    err.set_error(result.error_code(), result.error_message());
    replication_message::send(io, err);
    io.flush();
}

void channel_handler_base::process_loop(socket_io& io) {
    while (true) {
        auto message = replication_message::receive(io);
        dispatch(*message, io);
    }
}

}  // namespace limestone::replication
