/*
 * Copyright 2026-2026 Project Tsurugi.
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
#include <cstdint>
#include <string_view>

namespace limestone::replication::primitive_wire_codec {

/**
 * @brief Encode an 8-bit unsigned integer into wire format.
 * @param value Host-order value to encode.
 * @return One-byte encoded representation.
 */
[[nodiscard]] std::array<char, sizeof(std::uint8_t)> encode_uint8(std::uint8_t value) noexcept;

/**
 * @brief Encode a 16-bit unsigned integer into network byte order.
 * @param value Host-order value to encode.
 * @return Two-byte encoded representation in network byte order.
 */
[[nodiscard]] std::array<char, sizeof(std::uint16_t)> encode_uint16(std::uint16_t value) noexcept;

/**
 * @brief Encode a 32-bit unsigned integer into network byte order.
 * @param value Host-order value to encode.
 * @return Four-byte encoded representation in network byte order.
 */
[[nodiscard]] std::array<char, sizeof(std::uint32_t)> encode_uint32(std::uint32_t value) noexcept;

/**
 * @brief Encode a 64-bit unsigned integer into network byte order.
 * @param value Host-order value to encode.
 * @return Eight-byte encoded representation in network byte order.
 */
[[nodiscard]] std::array<char, sizeof(std::uint64_t)> encode_uint64(std::uint64_t value) noexcept;

/**
 * @brief Decode an 8-bit unsigned integer from wire format.
 * @param bytes One-byte encoded representation.
 * @return Decoded host-order value.
 */
[[nodiscard]] std::uint8_t decode_uint8(std::string_view bytes) noexcept;

/**
 * @brief Decode a 16-bit unsigned integer from network byte order.
 * @param bytes Two-byte encoded representation in network byte order.
 * @return Decoded host-order value.
 */
[[nodiscard]] std::uint16_t decode_uint16(std::string_view bytes) noexcept;

/**
 * @brief Decode a 32-bit unsigned integer from network byte order.
 * @param bytes Four-byte encoded representation in network byte order.
 * @return Decoded host-order value.
 */
[[nodiscard]] std::uint32_t decode_uint32(std::string_view bytes) noexcept;

/**
 * @brief Decode a 64-bit unsigned integer from network byte order.
 * @param bytes Eight-byte encoded representation in network byte order.
 * @return Decoded host-order value.
 */
[[nodiscard]] std::uint64_t decode_uint64(std::string_view bytes) noexcept;

}  // namespace limestone::replication::primitive_wire_codec
