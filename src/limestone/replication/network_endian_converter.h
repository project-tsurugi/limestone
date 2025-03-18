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
#include <iostream>
#include "../limestone_exception_helper.h"

namespace limestone::replication {

class network_endian_converter {
public:
    // Write to the stream with byte order conversion
    static inline void send_uint16(std::ostream& os, uint16_t value) {
        value = htons(value);  // Convert to network byte order
        os.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    static inline void send_uint32(std::ostream& os, uint32_t value) {
        value = htonl(value);  // Convert to network byte order
        os.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    static inline void send_uint64(std::ostream& os, uint64_t value) {
        uint32_t high = htonl(static_cast<uint32_t>(value >> 32));  // Convert high 32 bits to network byte order
        uint32_t low = htonl(static_cast<uint32_t>(value & 0xFFFFFFFF));  // Convert low 32 bits to network byte order
        os.write(reinterpret_cast<const char*>(&high), sizeof(high));
        os.write(reinterpret_cast<const char*>(&low), sizeof(low));
    }

    // Read from the stream with byte order conversion and error checking
    static inline uint16_t receive_uint16(std::istream& is) {
        uint16_t value;
        is.read(reinterpret_cast<char*>(&value), sizeof(value));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t value from stream", errno);
        }
        return ntohs(value);  // Convert from network byte order
    }

    static inline uint32_t receive_uint32(std::istream& is) {
        uint32_t value;
        is.read(reinterpret_cast<char*>(&value), sizeof(value));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t value from stream", errno);
        }
        return ntohl(value);  // Convert from network byte order
    }

    static inline uint64_t receive_uint64(std::istream& is) {
        uint32_t high, low;
        is.read(reinterpret_cast<char*>(&high), sizeof(high));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t value from stream", errno);
        }
        is.read(reinterpret_cast<char*>(&low), sizeof(low));
        if (!is) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t value from stream", errno);
        }
        uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32) | ntohl(low);  // Convert to host byte order
        return value;
    }
};

}  // namespace limestone::replication
