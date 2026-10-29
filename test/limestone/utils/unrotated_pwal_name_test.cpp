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

#include "internal.h"

#include "test_root.h"

namespace limestone::testing {

using limestone::internal::is_unrotated_pwal_name;

TEST(unrotated_pwal_name_test, matches_only_unrotated_pwal_names) { // NOLINT
    EXPECT_TRUE(is_unrotated_pwal_name("pwal_0000"));
    EXPECT_TRUE(is_unrotated_pwal_name("pwal_9999"));
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_0000.00000000000001.0"));  // rotated naming
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_000"));    // too short
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_00000"));  // too long
    EXPECT_FALSE(is_unrotated_pwal_name("pwal0000"));    // prefix "pwal_" does not match
    EXPECT_FALSE(is_unrotated_pwal_name("qwal_0000"));
    EXPECT_FALSE(is_unrotated_pwal_name("epoch"));
    EXPECT_FALSE(is_unrotated_pwal_name(""));
}

}  // namespace limestone::testing
