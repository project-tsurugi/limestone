/*
 * Copyright 2022-2026 Project Tsurugi.
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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal {

/**
 * @brief A buffer of a fixed size that packs fixed-length fields of an entry one after another in little-endian.
 *
 * Used by the typed writers to write the fixed-length fields that come one after another with one fwrite.
 * @tparam Capacity the maximum number of bytes that can be packed
 */
template <std::size_t Capacity>
class field_buffer {
public:
    /**
     * @brief Packs a 1-byte value.
     * @param value the value
     */
    void append_uint8(std::uint8_t value) {
        append(&value, sizeof(value));
    }

    /**
     * @brief Packs a 4-byte value in little-endian.
     * @param value the value
     */
    void append_uint32le(std::uint32_t value) {
        const std::uint32_t le = htole32(value);
        append(&le, sizeof(le));
    }

    /**
     * @brief Packs an 8-byte value in little-endian.
     * @param value the value
     */
    void append_uint64le(std::uint64_t value) {
        const std::uint64_t le = htole64(value);
        append(&le, sizeof(le));
    }

    /**
     * @brief Returns the beginning of the packed bytes.
     */
    [[nodiscard]] const char* data() const noexcept {
        return bytes_.data();
    }

    /**
     * @brief Returns the number of packed bytes.
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

private:
    /**
     * @brief Packs bytes. Writes a FATAL log and aborts if they exceed Capacity.
     * @param src the bytes to pack
     * @param len the number of bytes to pack
     */
    void append(const void* src, std::size_t len) {
        if (len > Capacity - size_) {
            LOG_LP(FATAL) << "field_buffer overflow: size " << size_ << " + " << len << " exceeds " << Capacity;
        }
        std::memcpy(bytes_.data() + size_, src, len);  // NOLINT(*-pointer-arithmetic)
        size_ += len;
    }

    std::array<char, Capacity> bytes_{};
    std::size_t size_{};
};

}  // namespace limestone::internal
