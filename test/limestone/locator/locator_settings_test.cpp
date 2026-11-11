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
#include <cstdint>

#include "test_root.h"

#include "locator/locator_settings.h"

namespace limestone::testing {

using limestone::internal::locator_settings;

// The defaults are the record interval of 32 MiB and R = 1
TEST(locator_settings_test, defaults) {
    const locator_settings settings{};
    EXPECT_EQ(settings.record_interval_bytes, std::uint64_t{32} * 1024 * 1024);
    EXPECT_EQ(settings.region_records, 1U);
}

}  // namespace limestone::testing
