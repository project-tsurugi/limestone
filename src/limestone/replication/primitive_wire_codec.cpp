#include "primitive_wire_codec.h"

#include <arpa/inet.h>

#include <cstddef>
#include <cstring>
#include <iterator>

namespace limestone::replication::primitive_wire_codec {

std::array<char, sizeof(std::uint8_t)> encode_uint8(std::uint8_t value) noexcept {
    std::array<char, sizeof(value)> buffer{};
    std::memcpy(buffer.data(), &value, sizeof(value));
    return buffer;
}

std::array<char, sizeof(std::uint16_t)> encode_uint16(std::uint16_t value) noexcept {
    std::uint16_t net_value = htons(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    return buffer;
}

std::array<char, sizeof(std::uint32_t)> encode_uint32(std::uint32_t value) noexcept {
    std::uint32_t net_value = htonl(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    return buffer;
}

std::array<char, sizeof(std::uint64_t)> encode_uint64(std::uint64_t value) noexcept {
    constexpr std::uint64_t mask32 = 0xFFFFFFFFULL;
    std::uint32_t high = htonl(static_cast<std::uint32_t>(value >> 32U));
    std::uint32_t low = htonl(static_cast<std::uint32_t>(value & mask32));
    std::array<char, sizeof(high) + sizeof(low)> buffer{};
    std::memcpy(buffer.data(), &high, sizeof(high));
    std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(high))), &low, sizeof(low));
    return buffer;
}

std::uint8_t decode_uint8(std::string_view bytes) noexcept {
    std::uint8_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return value;
}

std::uint16_t decode_uint16(std::string_view bytes) noexcept {
    std::uint16_t net_value = 0;
    std::memcpy(&net_value, bytes.data(), sizeof(net_value));
    return ntohs(net_value);
}

std::uint32_t decode_uint32(std::string_view bytes) noexcept {
    std::uint32_t net_value = 0;
    std::memcpy(&net_value, bytes.data(), sizeof(net_value));
    return ntohl(net_value);
}

std::uint64_t decode_uint64(std::string_view bytes) noexcept {
    std::uint32_t high = 0;
    std::uint32_t low = 0;
    std::memcpy(&high, bytes.data(), sizeof(high));
    auto const low_view = bytes.substr(sizeof(high), sizeof(low));
    std::memcpy(&low, low_view.data(), sizeof(low));
    return (static_cast<std::uint64_t>(ntohl(high)) << 32U) | static_cast<std::uint64_t>(ntohl(low));
}

}  // namespace limestone::replication::primitive_wire_codec
