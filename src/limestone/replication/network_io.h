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
 #include <array>
 #include <cstdint>
 #include <cstring>      // std::memcpy
 #include <iostream>
 #include "../limestone_exception_helper.h"
 
 namespace limestone::replication {
 
 class network_io {
 public:
     // Write to the stream with byte order conversion using std::array
     static inline void send_uint16(std::ostream &os, uint16_t value) {
         uint16_t net_value = htons(value);  // Convert to network byte order
         std::array<char, sizeof(net_value)> buffer{};
         std::memcpy(buffer.data(), &net_value, buffer.size());
         os.write(buffer.data(), buffer.size());
     }
 
     static inline void send_uint32(std::ostream &os, uint32_t value) {
         uint32_t net_value = htonl(value);  // Convert to network byte order
         std::array<char, sizeof(net_value)> buffer{};
         std::memcpy(buffer.data(), &net_value, buffer.size());
         os.write(buffer.data(), buffer.size());
     }
 
     static inline void send_uint64(std::ostream &os, uint64_t value) {
        constexpr uint64_t mask32 = 0xFFFFFFFFULL;
        uint32_t high = htonl(static_cast<uint32_t>(value >> 32U));
        uint32_t low  = htonl(static_cast<uint32_t>(value & mask32));
         {
             std::array<char, sizeof(high)> buffer{};
             std::memcpy(buffer.data(), &high, buffer.size());
             os.write(buffer.data(), buffer.size());
         }
         {
             std::array<char, sizeof(low)> buffer{};
             std::memcpy(buffer.data(), &low, buffer.size());
             os.write(buffer.data(), buffer.size());
         }
     }
 
     // Read from the stream with byte order conversion using std::array
     static inline uint16_t receive_uint16(std::istream &is) {
         uint16_t net_value{};
         std::array<char, sizeof(net_value)> buffer{};
         is.read(buffer.data(), buffer.size());
         if (!is) {
             LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t value from stream", errno);
         }
         std::memcpy(&net_value, buffer.data(), buffer.size());
         return ntohs(net_value);  // Convert from network byte order
     }
 
     static inline uint32_t receive_uint32(std::istream &is) {
         uint32_t net_value{};
         std::array<char, sizeof(net_value)> buffer{};
         is.read(buffer.data(), buffer.size());
         if (!is) {
             LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t value from stream", errno);
         }
         std::memcpy(&net_value, buffer.data(), buffer.size());
         return ntohl(net_value);  // Convert from network byte order
     }
 
     static inline uint64_t receive_uint64(std::istream &is) {
        uint32_t high{};
        uint32_t low{};
        {
            std::array<char, sizeof(high)> buffer{};
            is.read(buffer.data(), buffer.size());
            if (!is) {
                LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t value from stream", errno);
            }
            std::memcpy(&high, buffer.data(), buffer.size());
        }
        {
            std::array<char, sizeof(low)> buffer{};
            is.read(buffer.data(), buffer.size());
            if (!is) {
                LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t value from stream", errno);
            }
            std::memcpy(&low, buffer.data(), buffer.size());
        }
        uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32U)
                         | static_cast<uint64_t>(ntohl(low));
        return value;
    }

    static inline void send_string(std::ostream &os, const std::string &value) {
        send_uint32(os, static_cast<uint32_t>(value.size()));
        os.write(value.data(), value.size());
        if (!os) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to write string to stream", errno);
        }
    }
    
    static inline std::string receive_string(std::istream &is) {
        uint32_t len = receive_uint32(is);
        std::string result;
        result.resize(len);
        is.read(result.data(), len);
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read string body from stream", errno);
        }
        return result;
    }

    static inline void send_uint8(std::ostream &os, uint8_t value) {
        std::array<char, sizeof(value)> buffer{};
        std::memcpy(buffer.data(), &value, buffer.size());
        os.write(buffer.data(), buffer.size());
        if (!os) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to write uint8_t to stream", errno);
        }
    }
    
    static inline uint8_t receive_uint8(std::istream &is) {
        uint8_t value{};
        std::array<char, sizeof(value)> buffer{};
        is.read(buffer.data(), buffer.size());
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read uint8_t from stream", errno);
        }
        std::memcpy(&value, buffer.data(), buffer.size());
        return value;
    }
 };
 
 }  // namespace limestone::replication