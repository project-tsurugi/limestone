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

#include <limestone/api/log_channel.h>

#include <atomic>

#include "channel_handler_base.h"

namespace limestone::replication {

using limestone::api::log_channel;
class log_channel_handler : public channel_handler_base {
public:
    static constexpr int MAX_LOG_CHANNEL_COUNT = 100000;

    explicit log_channel_handler(replica_server &server, socket_io& io) noexcept;
    ~log_channel_handler() override = default;

    // Delete copy and move constructors and assignment operators
    log_channel_handler(const log_channel_handler &) = delete;
    log_channel_handler &operator=(const log_channel_handler &) = delete;
    log_channel_handler(log_channel_handler &&) = delete;
    log_channel_handler &operator=(log_channel_handler &&) = delete;

    /**
     * @brief Set the internal log_channel_id_counter to a specific value for testing.
     * This method is for testing purposes only.
     */
    void set_log_channel_id_counter_for_test(int value);

    /**
     * @brief Get the log channel associated with this handler.
     */
    [[nodiscard]] log_channel& get_log_channel();

protected:
    // Assign a log channel and set the thread name.
    validation_result authorize() override; 

    // Validate the initial message of the channel.
    validation_result validate_initial(std::unique_ptr<replication_message> request) override;
    
    // Send the initial acknowledgement message.
    void send_initial_ack() const override;
    
    // Dispatch further messages.
    void dispatch(replication_message &message, handler_resources& resources) override;
    
private:
    std::atomic<int> log_channel_id_counter{0};
    log_channel* log_channel_{nullptr}; 
};

} // namespace limestone::replication