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

 #include "replication_message.h"
 
 namespace limestone::replication {
 
 // Derived class implementing the pure virtual functions of replication_message
 class session_begin_message : public replication_message {
 public:
     void send_body(std::ostream& os) const override;
     void receive_body(std::istream& is) override;
 
     [[nodiscard]] message_type_id get_message_type_id() const override;
 
     // Factory function for creating session_begin_message
     [[nodiscard]] static std::shared_ptr<replication_message> create();


         // Process the message after it has been received.
    // Empty implementation for session_begin_message
    void post_receive() override {
        // No specific processing needed for session_begin_message
    }

    // Retrieve internal data for testing purposes.
    // Empty implementation for session_begin_message, returns an empty string.
    [[nodiscard]] std::string get_data_for_testing() const override {
        return "";
    }
 };
 
 }  // namespace limestone::replication
 