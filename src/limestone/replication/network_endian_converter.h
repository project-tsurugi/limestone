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

 #include <arpa/inet.h>
 #include <cstdint>
 #include <cstring>      // std::memcpy
 #include <iostream>
 #include "../limestone_exception_helper.h"
 
 namespace limestone::replication {
 
 class network_endian_converter {
 public:
     // Write to the stream with byte order conversion without using reinterpret_cast
     static inline void send_uint16(std::ostream& os, uint16_t value) {
         uint16_t net_value = htons(value);  // Convert to network byte order
         char buffer[sizeof(net_value)];
         std::memcpy(buffer, &net_value, sizeof(net_value));
         os.write(buffer, sizeof(net_value));
     }
 
     static inline void send_uint32(std::ostream& os, uint32_t value) {
         uint32_t net_value = htonl(value);  // Convert to network byte order
         char buffer[sizeof(net_value)];
         std::memcpy(buffer, &net_value, sizeof(net_value));
         os.write(buffer, sizeof(net_value));
     }
 
     static inline void send_uint64(std::ostream& os, uint64_t value) {
         // Use unsigned literal to avoid warnings with bitwise operators
         uint32_t high = htonl(static_cast<uint32_t>(value >> 32));  
         uint32_t low = htonl(static_cast<uint32_t>(value & 0xFFFFFFFFULL));  
         {
              char buffer[sizeof(high)];
              std::memcpy(buffer, &high, sizeof(high));
              os.write(buffer, sizeof(high));
         }
         {
              char buffer[sizeof(low)];
              std::memcpy(buffer, &low, sizeof(low));
              os.write(buffer, sizeof(low));
         }
     }
 
     // Read from the stream with byte order conversion and error checking without using reinterpret_cast
     static inline uint16_t receive_uint16(std::istream &is) {
         uint16_t net_value{};
         char buffer[sizeof(net_value)] = {};
         is.read(buffer, sizeof(net_value));
         if (!is) {
             LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t value from stream", errno);
         }
         std::memcpy(&net_value, buffer, sizeof(net_value));
         return ntohs(net_value);  // Convert from network byte order
     }
 
     static inline uint32_t receive_uint32(std::istream &is) {
         uint32_t net_value{};
         char buffer[sizeof(net_value)] = {};
         is.read(buffer, sizeof(net_value));
         if (!is) {
             LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t value from stream", errno);
         }
         std::memcpy(&net_value, buffer, sizeof(net_value));
         return ntohl(net_value);  // Convert from network byte order
     }
 
     static inline uint64_t receive_uint64(std::istream &is) {
         uint32_t high{};
         uint32_t low{};
         {
             char buffer[sizeof(high)] = {};
             is.read(buffer, sizeof(high));
             if (!is) {
                 LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t value from stream", errno);
             }
             std::memcpy(&high, buffer, sizeof(high));
         }
         {
             char buffer[sizeof(low)] = {};
             is.read(buffer, sizeof(low));
             if (!is) {
                 LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t value from stream", errno);
             }
             std::memcpy(&low, buffer, sizeof(low));
         }
         uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32) | ntohl(low);  // Convert to host byte order
         return value;
     }
 };
 
 }  // namespace limestone::replication