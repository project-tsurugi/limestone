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

#include "replication/control_channel_handler.h"
#include "socket_io.h"
#include "limestone_exception_helper.h"
#include "logging_helper.h"

namespace limestone::replication {

control_channel_handler::control_channel_handler(replica_server& server, socket_io& io) noexcept
     : channel_handler_base(server, io) {}

validation_result control_channel_handler::authorize() {
    TRACE_START;
    // TODO その他の認証を実装する
    if (!get_server().mark_control_channel_created()) {
        LOG_LP(ERROR) << "Control channel already created";
        return validation_result::error(1, "Control channel already created");
    }
    pthread_setname_np(pthread_self(), "limestone-ctrl");
    TRACE_END << "Thread name set to limestone-ctrl";
    return validation_result::success();
}

validation_result control_channel_handler::validate_initial(std::unique_ptr<replication_message> request) {
    if (request->get_message_type_id() != message_type_id::SESSION_BEGIN) {
        std::ostringstream msg;
        msg << "Invalid message type: " << static_cast<int>(request->get_message_type_id())
            << ", expected SESSION_BEGIN";
        return validation_result::error(2, msg.str());
    }

    auto* msg = dynamic_cast<message_session_begin*>(request.get());
    if (!msg) {
        return validation_result::error(3, "Failed to cast to message_session_begin");
    }

    // TODO: validate fields inside message_session_begin (protocol_version, configuration_id, epoch_number)

    return validation_result::success();
}
 
 void control_channel_handler::send_initial_ack() const {
     message_session_begin_ack ack;
     ack.set_session_secret("server_.get_session_secret()"); // TODO: actual secret
     replication_message::send(get_socket_io(), ack);
     get_socket_io().flush();
 }
 
 void control_channel_handler::dispatch(replication_message& /*message*/, handler_resources& /*resources*/) {
     // TODO: implement control message dispatch logic
 }

 }  // namespace limestone::replication
 