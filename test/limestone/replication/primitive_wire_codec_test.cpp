#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "replication/primitive_wire_codec.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(primitive_wire_codec_test, encodes_in_network_byte_order) {
    auto const encoded_uint8 = primitive_wire_codec::encode_uint8(0x12U);
    EXPECT_EQ(encoded_uint8, (std::array<char, 1>{char{0x12}}));

    auto const encoded_uint16 = primitive_wire_codec::encode_uint16(0x1234U);
    EXPECT_EQ(encoded_uint16, (std::array<char, 2>{char{0x12}, char{0x34}}));

    auto const encoded_uint32 = primitive_wire_codec::encode_uint32(0x12345678U);
    EXPECT_EQ(encoded_uint32, (std::array<char, 4>{char{0x12}, char{0x34}, char{0x56}, char{0x78}}));

    auto const encoded_uint64 = primitive_wire_codec::encode_uint64(0x123456789ABCDEF0ULL);
    EXPECT_EQ(encoded_uint64,
            (std::array<char, 8>{
                    char{0x12}, char{0x34}, char{0x56}, char{0x78},
                    static_cast<char>(0x9A), static_cast<char>(0xBC),
                    static_cast<char>(0xDE), static_cast<char>(0xF0)}));
}

TEST(primitive_wire_codec_test, decodes_from_network_byte_order) {
    std::array<char, 1> const encoded_uint8{char{0x12}};
    EXPECT_EQ(primitive_wire_codec::decode_uint8(std::string_view{encoded_uint8.data(), encoded_uint8.size()}), 0x12U);

    std::array<char, 2> const encoded_uint16{char{0x12}, char{0x34}};
    EXPECT_EQ(primitive_wire_codec::decode_uint16(std::string_view{encoded_uint16.data(), encoded_uint16.size()}), 0x1234U);

    std::array<char, 4> const encoded_uint32{char{0x12}, char{0x34}, char{0x56}, char{0x78}};
    EXPECT_EQ(primitive_wire_codec::decode_uint32(std::string_view{encoded_uint32.data(), encoded_uint32.size()}), 0x12345678U);

    std::array<char, 8> const encoded_uint64{
            char{0x12}, char{0x34}, char{0x56}, char{0x78},
            static_cast<char>(0x9A), static_cast<char>(0xBC),
            static_cast<char>(0xDE), static_cast<char>(0xF0)};
    EXPECT_EQ(primitive_wire_codec::decode_uint64(std::string_view{encoded_uint64.data(), encoded_uint64.size()}),
            0x123456789ABCDEF0ULL);
}

}  // namespace limestone::testing
