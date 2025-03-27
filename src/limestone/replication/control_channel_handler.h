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

 #include <atomic>
 #include "replication/channel_handler_base.h"
 #include "replication/message_session_begin.h"
 #include "replication/message_session_begin_ack.h"

namespace limestone::replication {

class control_channel_handler : public channel_handler_base {
public:
    explicit control_channel_handler(replica_server& server) noexcept;

protected:
    validation_result authorize() override;
    validation_result validate_initial(std::unique_ptr<replication_message> request) override;
    void send_initial_ack(socket_io& io) const override;
    void dispatch(replication_message& message, socket_io& io) override;

private:
    std::atomic<bool> has_received_session_begin_{false};
};

}  // namespace limestone::replication
 