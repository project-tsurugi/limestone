/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include "replication/validation_result.h"
#include "gtest/gtest.h"

namespace limestone::testing {
using namespace limestone::replication;

TEST(validation_result_test, success_case) {
    auto result = validation_result::success();
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.error_code(), 0u);
    EXPECT_EQ(result.error_message(), "");
}

TEST(validation_result_test, error_case) {
    uint16_t code = 123;
    std::string msg = "validation failed";

    auto result = validation_result::error(code, msg);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), code);
    EXPECT_EQ(result.error_message(), msg);
}

TEST(validation_result_test, immutability_of_result) {
    auto result = validation_result::error(7, "fatal");
    // original remains unchanged
    EXPECT_EQ(result.error_code(), 7);
    EXPECT_EQ(result.error_message(), "fatal");
    EXPECT_FALSE(result.ok());
}

}  // namespace limestone::testing
