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
 
 class message_ack : public replication_message {
 public:
     [[nodiscard]] message_type_id get_message_type_id() const override;
     void send_body(socket_io& io) const override;
     void receive_body(socket_io& io) override;
 
     [[nodiscard]] static std::unique_ptr<replication_message> create();
 
 private:
    // Register COMMON_ACK in replication_message factory map.
    // The static initialization here is intentional. If an exception occurs,
    // the program should terminate immediately. We ignore the clang-tidy warning 
    // (cert-err58-cpp) as this behavior is desired.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
         replication_message::register_message_type(
             message_type_id::COMMON_ACK, &message_ack::create);
         return true;
     }();
 };
 
 }  // namespace limestone::replication
