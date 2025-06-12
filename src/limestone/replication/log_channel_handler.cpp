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

#include "log_channel_handler.h"

#include "replication_message.h"
#include "message_ack.h"
#include "message_error.h"
#include "message_log_channel_create.h"
#include "validation_result.h"
#include "socket_io.h"
#include "logging_helper.h"
#include <glog/logging.h>

namespace limestone::replication {

log_channel_handler::log_channel_handler(replica_server &server, socket_io& io) noexcept
    : channel_handler_base(server, io){}

validation_result log_channel_handler::validate_initial(std::unique_ptr<replication_message> request) {
    if (request->get_message_type_id() != message_type_id::LOG_CHANNEL_CREATE) {
        std::ostringstream msg;
        msg << "Invalid message type: " << static_cast<int>(request->get_message_type_id()) << ", expected LOG_CHANNEL_CREATE";
        return validation_result::error(2, msg.str());
    }

    auto *msg = dynamic_cast<message_log_channel_create*>(request.get());
    if (!msg) {
        return validation_result::error(3, "Failed to cast to message_log_channel_create");
    }

    // TODO その他のバリデーション処理を入れる

    auto& server = get_server();
    auto& ds = get_server().get_datastore();    
    log_channel_ = &ds.create_channel(server.get_location());
    

    // Perform additional validation as needed
    return validation_result::success();
}

void log_channel_handler::send_initial_ack() const {
    send_ack();
}

void log_channel_handler::dispatch(replication_message& message, handler_resources& resources) {
    message.post_receive(resources);
}

validation_result log_channel_handler::authorize() {
    int id = log_channel_id_counter.fetch_add(1, std::memory_order_seq_cst);
    if (id >= MAX_LOG_CHANNEL_COUNT) {
        LOG(ERROR) << "Exceeded maximum number of log channels: " << MAX_LOG_CHANNEL_COUNT;
        return validation_result::error(1, "Too many log channels: cannot assign more");
    }

    std::ostringstream oss;
    oss << "logch" << std::setw(5) << std::setfill('0') << id;
    pthread_setname_np(pthread_self(), oss.str().c_str());

    return validation_result::success();
}

void log_channel_handler::set_log_channel_id_counter_for_test(int value) {
    log_channel_id_counter.store(value, std::memory_order_seq_cst);
}

log_channel& log_channel_handler::get_log_channel() {
    return *log_channel_;
}

std::unique_ptr<handler_resources> log_channel_handler::create_handler_resources() {
    return std::make_unique<log_channel_handler_resources>(get_socket_io(), *log_channel_);
}

} // namespace limestone::replication