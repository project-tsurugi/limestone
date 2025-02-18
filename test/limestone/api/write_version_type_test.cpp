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

#include "limestone/api/write_version_type.h"

#include <gtest/gtest.h>

namespace limestone::testing {

using namespace limestone::api;

TEST(write_version_type_test, comprehensive_comparison) {
    // Base value: major = 10, minor = 5.
    write_version_type base(10, 5);

    // Verify 9 combinations of major: 9, 10, 11; minor: 4, 5, 6.
    for (int major = 9; major <= 11; major++) {
        for (int minor = 4; minor <= 6; minor++) {
            write_version_type test_val(major, minor);
            // Verification of operator==
            if (major == 10 && minor == 5) {
                // Exact match case
                EXPECT_TRUE(base == test_val) << "Expected equality for (10,5) vs (" << major << "," << minor << ")";
            } else {
                EXPECT_FALSE(base == test_val) << "Expected inequality for (10,5) vs (" << major << "," << minor << ")";
            }

            // Verification of operator<
            bool expected_less = false;
            // Conditions for base < test_val:
            // - major is greater than base, or
            // - major is equal and minor is greater than base
            if ((10 < major) || (10 == major && 5 < minor)) {
                expected_less = true;
            }
            EXPECT_EQ(base < test_val, expected_less) << "Comparison failed for (10,5) < (" << major << "," << minor << ")";

            // Verification of operator<=
            // base <= test_val is equivalent to (base < test_val) or (base == test_val)
            EXPECT_EQ(base <= test_val, (expected_less || (major == 10 && minor == 5))) << "Comparison failed for (10,5) <= (" << major << "," << minor << ")";
        }
    }
}

TEST(write_version_type_test, equality_operator) {
    // Test default constructed objects.
    write_version_type wv_default1;
    write_version_type wv_default2;
    EXPECT_TRUE(wv_default1 == wv_default2);

    // Test equality for same epoch and minor version.
    write_version_type wv1(10, 5);
    write_version_type wv2(10, 5);
    EXPECT_TRUE(wv1 == wv2);

    // Test inequality on minor version difference.
    write_version_type wv3(10, 6);
    EXPECT_FALSE(wv1 == wv3);

    // Test inequality on epoch difference.
    write_version_type wv4(9, 100);
    EXPECT_FALSE(wv1 == wv4);
}

TEST(write_version_type_test, less_than_operator) {
    // Same epoch, different minor version.
    write_version_type wv1(10, 5);
    write_version_type wv2(10, 6);
    EXPECT_TRUE(wv1 < wv2);
    EXPECT_FALSE(wv2 < wv1);

    // Different epochs.
    write_version_type wv3(11, 1);
    write_version_type wv4(9, 100);
    EXPECT_TRUE(wv1 < wv3);
    EXPECT_TRUE(wv4 < wv1);
}

TEST(write_version_type_test, less_than_or_equal_operator) {
    write_version_type wv1(10, 5);
    write_version_type wv_equal(10, 5);
    write_version_type wv_higher_minor(10, 6);
    write_version_type wv_higher_epoch(11, 1);
    write_version_type wv_lower_epoch(9, 100);

    // Equal case.
    EXPECT_TRUE(wv1 <= wv_equal);

    // Less-than cases.
    EXPECT_TRUE(wv1 <= wv_higher_minor);
    EXPECT_TRUE(wv1 <= wv_higher_epoch);
    EXPECT_TRUE(wv_lower_epoch <= wv1);

    // Greater-than case.
    EXPECT_FALSE(wv_higher_minor <= wv1);
}

} // namespace limestone::testing
