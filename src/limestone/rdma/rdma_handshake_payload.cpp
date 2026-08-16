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
#include <rdma/rdma_handshake_payload.h>

#include <array>
#include <ios>
#include <iterator>
#include <limits>
#include <string_view>

#include <limestone_exception_helper.h>
#include <replication/primitive_wire_codec.h>

namespace limestone::replication {

namespace {

[[nodiscard]] std::uint32_t checked_string_size(std::size_t size) {
    if (size > std::numeric_limits<std::uint32_t>::max()) {
        LOG_AND_THROW_EXCEPTION("handshake payload string is too large");
    }
    return static_cast<std::uint32_t>(size);
}

void append_bytes(std::vector<std::uint8_t>& out, char const* data, std::size_t size) {
    out.insert(out.end(), data, std::next(data, static_cast<std::ptrdiff_t>(size)));
}

template <std::size_t Size>
void append(std::vector<std::uint8_t>& out, std::array<char, Size> const& bytes) {
    append_bytes(out, bytes.data(), bytes.size());
}

void append_string(std::vector<std::uint8_t>& out, std::string const& value) {
    append(out, primitive_wire_codec::encode_uint32(checked_string_size(value.size())));
    append_bytes(out, value.data(), value.size());
}

class payload_reader {
public:
    explicit payload_reader(std::vector<std::uint8_t> const& bytes)
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        : remaining_(reinterpret_cast<char const*>(bytes.data()), bytes.size()) {}

    [[nodiscard]] bool read_uint8(std::uint8_t& value) {
        std::string_view view;
        if (!take(sizeof(value), view)) {
            return false;
        }
        value = primitive_wire_codec::decode_uint8(view);
        return true;
    }

    [[nodiscard]] bool read_uint16(std::uint16_t& value) {
        std::string_view view;
        if (!take(sizeof(value), view)) {
            return false;
        }
        value = primitive_wire_codec::decode_uint16(view);
        return true;
    }

    [[nodiscard]] bool read_uint32(std::uint32_t& value) {
        std::string_view view;
        if (!take(sizeof(value), view)) {
            return false;
        }
        value = primitive_wire_codec::decode_uint32(view);
        return true;
    }

    [[nodiscard]] bool read_uint64(std::uint64_t& value) {
        std::string_view view;
        if (!take(sizeof(value), view)) {
            return false;
        }
        value = primitive_wire_codec::decode_uint64(view);
        return true;
    }

    [[nodiscard]] bool read_string(std::string& value) {
        std::uint32_t size{};
        if (!read_uint32(size)) {
            return false;
        }
        std::string_view view;
        if (!take(size, view)) {
            return false;
        }
        value.assign(view);
        return true;
    }

    [[nodiscard]] bool finished() const {
        return remaining_.empty();
    }

private:
    [[nodiscard]] bool take(std::size_t count, std::string_view& view) {
        if (remaining_.size() < count) {
            return false;
        }
        view = remaining_.substr(0, count);
        remaining_.remove_prefix(count);
        return true;
    }

    std::string_view remaining_;
};

} // namespace

bool operator==(rdma_handshake_start_payload const& lhs, rdma_handshake_start_payload const& rhs) {
    return lhs.protocol_version == rhs.protocol_version &&
        lhs.configuration_id == rhs.configuration_id && lhs.epoch_number == rhs.epoch_number &&
        lhs.slot_count == rhs.slot_count && lhs.master_dma_address == rhs.master_dma_address &&
        lhs.channel_count == rhs.channel_count && lhs.control_channel_id == rhs.control_channel_id;
}

bool operator!=(rdma_handshake_start_payload const& lhs, rdma_handshake_start_payload const& rhs) {
    return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& out, rdma_handshake_start_payload const& value) {
    auto const saved_flags = out.flags();
    out << "rdma_handshake_start_payload{protocol_version=" << value.protocol_version
        << ", configuration_id=\"" << value.configuration_id << "\""
        << ", epoch_number=" << value.epoch_number << ", slot_count=" << value.slot_count
        << ", master_dma_address=0x" << std::hex << value.master_dma_address << std::dec
        << ", channel_count=" << value.channel_count
        << ", control_channel_id=" << value.control_channel_id << "}";
    out.flags(saved_flags);
    return out;
}

bool operator==(
        rdma_handshake_response_payload const& lhs, rdma_handshake_response_payload const& rhs) {
    return lhs.accepted == rhs.accepted && lhs.error_message == rhs.error_message &&
        lhs.replica_dma_address == rhs.replica_dma_address;
}

bool operator!=(
        rdma_handshake_response_payload const& lhs, rdma_handshake_response_payload const& rhs) {
    return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& out, rdma_handshake_response_payload const& value) {
    auto const saved_flags = out.flags();
    out << "rdma_handshake_response_payload{accepted=" << std::boolalpha << value.accepted
        << ", error_message=\"" << value.error_message << "\""
        << ", replica_dma_address=0x" << std::hex << value.replica_dma_address << "}";
    out.flags(saved_flags);
    return out;
}

std::vector<std::uint8_t> encode(rdma_handshake_start_payload const& payload) {
    std::vector<std::uint8_t> out;
    out.reserve(sizeof(std::uint64_t) * 3 + sizeof(std::uint32_t) * 2 +
        sizeof(std::uint16_t) * 2 + payload.configuration_id.size());
    append(out, primitive_wire_codec::encode_uint64(payload.protocol_version));
    append_string(out, payload.configuration_id);
    append(out, primitive_wire_codec::encode_uint64(payload.epoch_number));
    append(out, primitive_wire_codec::encode_uint32(payload.slot_count));
    append(out, primitive_wire_codec::encode_uint64(payload.master_dma_address));
    append(out, primitive_wire_codec::encode_uint16(payload.channel_count));
    append(out, primitive_wire_codec::encode_uint16(payload.control_channel_id));
    return out;
}

std::vector<std::uint8_t> encode(rdma_handshake_response_payload const& payload) {
    std::vector<std::uint8_t> out;
    out.reserve(sizeof(std::uint8_t) + sizeof(std::uint32_t) + sizeof(std::uint64_t) +
        payload.error_message.size());
    append(out, primitive_wire_codec::encode_uint8(payload.accepted ? 1U : 0U));
    append_string(out, payload.error_message);
    append(out, primitive_wire_codec::encode_uint64(payload.replica_dma_address));
    return out;
}

std::optional<rdma_handshake_start_payload> decode_start_payload(
        std::vector<std::uint8_t> const& bytes) {
    payload_reader reader{bytes};
    rdma_handshake_start_payload payload{};
    if (!reader.read_uint64(payload.protocol_version) ||
        !reader.read_string(payload.configuration_id) ||
        !reader.read_uint64(payload.epoch_number) || !reader.read_uint32(payload.slot_count) ||
        !reader.read_uint64(payload.master_dma_address) ||
        !reader.read_uint16(payload.channel_count) ||
        !reader.read_uint16(payload.control_channel_id) || !reader.finished()) {
        return std::nullopt;
    }
    return payload;
}

std::optional<rdma_handshake_response_payload> decode_response_payload(
        std::vector<std::uint8_t> const& bytes) {
    payload_reader reader{bytes};
    rdma_handshake_response_payload payload{};
    std::uint8_t accepted{};
    if (!reader.read_uint8(accepted) || accepted > 1U ||
        !reader.read_string(payload.error_message) ||
        !reader.read_uint64(payload.replica_dma_address) || !reader.finished()) {
        return std::nullopt;
    }
    payload.accepted = accepted != 0U;
    return payload;
}

} // namespace limestone::replication
