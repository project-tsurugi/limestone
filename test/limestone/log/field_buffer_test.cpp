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

#include <string>
#include <string_view>

#include "test_root.h"

#include "log_entry/field_buffer.h"
#include "log_entry_bytes.h"

namespace limestone::testing {

using limestone::internal::field_buffer;
using log_entry_bytes::u32le;
using log_entry_bytes::u64le;
using log_entry_bytes::u8;

// the packed values come one after another in little-endian
TEST(field_buffer_test, append) {
    field_buffer<13> buf{};
    EXPECT_EQ(buf.size(), 0U);
    buf.append_uint8(0x12);
    buf.append_uint32le(0x01020304);
    buf.append_uint64le(0x0102030405060708);
    EXPECT_EQ(std::string_view(buf.data(), buf.size()), u8(0x12) + u32le(0x01020304) + u64le(0x0102030405060708));
}

// packing beyond the size aborts
TEST(field_buffer_test, overflow_aborts) {
    field_buffer<4> buf{};
    buf.append_uint32le(1);
    EXPECT_DEATH(buf.append_uint8(1), "field_buffer overflow");
}

}  // namespace limestone::testing
